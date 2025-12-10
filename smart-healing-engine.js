// =============================================================================
// smart-healing-engine.js - Production-Ready Data Healing System
// =============================================================================
//
// Architecture (what professionals use):
// 
// 1. LIVE FEED (real-time)
//    - Finnhub WebSocket → immediate candle updates
//    - Quick, but may have spikes/gaps
//
// 2. HEALING WINDOW (every 5-10 min)
//    - Only fix last 15-20 minutes
//    - Nuclear DELETE+INSERT for small window
//    - Dukascopy has ~15-30 min delay, so this catches it
//
// 3. DAILY VERIFICATION (2 AM daily)
//    - Full previous day check
//    - Fill gaps, fix incomplete
//    - Uses INSERT IGNORE (doesn't overwrite good data)
//
// 4. HISTORICAL SEEDING (one-time/manual)
//    - Initial data population
//    - INSERT IGNORE (add missing, keep existing)
//
// =============================================================================

require('dotenv').config();

const cron = require('node-cron');
const { getHistoricalRates } = require('dukascopy-node');
const database = require('./database');
const { isMarketOpenForSymbol } = require('./config/market-hours');

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
    // Primary pairs - healed every 5 minutes
    primaryPairs: [
        'EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF',
        'AUDUSD', 'USDCAD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
    ],
    
    // Secondary pairs - healed every 15 minutes
    secondaryPairs: [
        'XAGUSD', 'EURCHF', 'GBPCHF', 'AUDJPY', 'EURAUD',
        'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
        'NZDJPY', 'CADJPY'
    ],
    
    // Healing window (minutes) - how far back to fix
    healingWindowMinutes: 20,
    
    // Dukascopy delay (minutes) - don't fix more recent than this
    dukascopyDelayMinutes: 15,
    
    // Timeframes
    timeframes: ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'],
};

const DUKASCOPY_INSTRUMENTS = {
    'EURUSD': 'eurusd', 'GBPUSD': 'gbpusd', 'USDJPY': 'usdjpy',
    'USDCHF': 'usdchf', 'AUDUSD': 'audusd', 'USDCAD': 'usdcad',
    'NZDUSD': 'nzdusd', 'EURGBP': 'eurgbp', 'EURJPY': 'eurjpy',
    'GBPJPY': 'gbpjpy', 'EURCHF': 'eurchf', 'GBPCHF': 'gbpchf',
    'AUDJPY': 'audjpy', 'EURAUD': 'euraud', 'EURCAD': 'eurcad',
    'GBPAUD': 'gbpaud', 'GBPCAD': 'gbpcad', 'AUDCAD': 'audcad',
    'AUDNZD': 'audnzd', 'NZDJPY': 'nzdjpy', 'CADJPY': 'cadjpy',
    'XAUUSD': 'xauusd', 'XAGUSD': 'xagusd',
};

// Track last heal time per symbol
const lastHealTime = new Map();

// =============================================================================
// CORE FUNCTIONS
// =============================================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function getSymbolType(symbol) {
    if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return 'metal';
    return 'forex';
}

/**
 * Fetch M1 candles from Dukascopy
 */
async function fetchM1FromDukascopy(symbol, from, to) {
    const instrument = DUKASCOPY_INSTRUMENTS[symbol];
    if (!instrument) return [];
    
    try {
        const data = await getHistoricalRates({
            instrument,
            dates: { from, to },
            timeframe: 'm1',
            format: 'json',
            priceType: 'bid',
            volumes: true,
        });
        
        return data.map(c => ({
            symbol,
            timeframe: 'M1',
            timestamp: new Date(c.timestamp),
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume || 0,
        }));
    } catch (error) {
        console.error(`❌ Dukascopy fetch error (${symbol}):`, error.message);
        return [];
    }
}

/**
 * DELETE M1 candles in range
 */
async function deleteM1Range(symbol, from, to) {
    const [result] = await database.pool.execute(`
        DELETE FROM pulse_market_data
        WHERE symbol = ? AND timeframe = 'M1'
        AND timestamp >= ? AND timestamp < ?
    `, [symbol, from, to]);
    return result.affectedRows;
}

/**
 * INSERT candles (plain insert, no upsert)
 */
async function insertCandles(candles) {
    if (candles.length === 0) return 0;
    
    const batchSize = 500;
    let inserted = 0;
    
    for (let i = 0; i < candles.length; i += batchSize) {
        const batch = candles.slice(i, i + batchSize);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, 0)').join(',');
        const values = batch.flatMap(c => [
            c.symbol, c.timeframe, c.timestamp,
            c.open, c.high, c.low, c.close, c.volume
        ]);
        
        try {
            const [result] = await database.pool.execute(`
                INSERT INTO pulse_market_data 
                (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                VALUES ${placeholders}
            `, values);
            inserted += result.affectedRows;
        } catch (error) {
            // Individual fallback for duplicates
            for (const c of batch) {
                try {
                    await database.pool.execute(`
                        INSERT IGNORE INTO pulse_market_data 
                        (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                    `, [c.symbol, c.timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume]);
                    inserted++;
                } catch (e) {}
            }
        }
    }
    return inserted;
}

/**
 * Rebuild higher timeframes from M1
 */
async function rebuildHigherTimeframes(symbol, from, to) {
    const timeframes = ['M5', 'M15', 'M30', 'H1', 'H4', 'D1'];
    const tfMinutes = { M5: 5, M15: 15, M30: 30, H1: 60, H4: 240, D1: 1440 };
    
    for (const tf of timeframes) {
        try {
            // Get M1 candles in range
            const [m1Candles] = await database.pool.execute(`
                SELECT timestamp, open, high, low, close, volume
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = 'M1'
                AND timestamp >= ? AND timestamp < ?
                ORDER BY timestamp
            `, [symbol, from, to]);
            
            if (m1Candles.length === 0) continue;
            
            // Group by period
            const periodMs = tfMinutes[tf] * 60 * 1000;
            const grouped = new Map();
            
            for (const candle of m1Candles) {
                const ts = new Date(candle.timestamp).getTime();
                const periodStart = Math.floor(ts / periodMs) * periodMs;
                
                if (!grouped.has(periodStart)) {
                    grouped.set(periodStart, []);
                }
                grouped.get(periodStart).push(candle);
            }
            
            // Aggregate and save each period
            for (const [periodKey, candles] of grouped) {
                const periodStart = new Date(parseInt(periodKey));
                candles.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
                
                const aggregated = {
                    symbol,
                    timeframe: tf,
                    timestamp: periodStart,
                    open: parseFloat(candles[0].open),
                    high: Math.max(...candles.map(c => parseFloat(c.high))),
                    low: Math.min(...candles.map(c => parseFloat(c.low))),
                    close: parseFloat(candles[candles.length - 1].close),
                    volume: candles.reduce((sum, c) => sum + parseFloat(c.volume || 0), 0),
                };
                
                // DELETE + INSERT for this period
                await database.pool.execute(`
                    DELETE FROM pulse_market_data
                    WHERE symbol = ? AND timeframe = ? AND timestamp = ?
                `, [symbol, tf, periodStart]);
                
                await database.pool.execute(`
                    INSERT INTO pulse_market_data
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                `, [symbol, tf, periodStart, aggregated.open, aggregated.high,
                    aggregated.low, aggregated.close, aggregated.volume]);
            }
        } catch (error) {
            console.error(`   ❌ Rebuild ${tf} error: ${error.message}`);
        }
    }
}

// =============================================================================
// HEALING WINDOW - Run every 5-10 minutes
// =============================================================================

/**
 * Heal the last N minutes for a symbol
 * This is the "smart nuclear" - only fixes recent window
 */
async function healSymbol(symbol) {
    const symbolType = getSymbolType(symbol);
    const marketStatus = isMarketOpenForSymbol(symbolType);
    
    // Skip if market is closed
    if (!marketStatus.open) {
        return { symbol, skipped: true, reason: 'market_closed' };
    }
    
    const now = new Date();
    
    // Calculate healing window
    // From: now - healingWindow - dukascopyDelay
    // To: now - dukascopyDelay
    const to = new Date(now.getTime() - CONFIG.dukascopyDelayMinutes * 60 * 1000);
    const from = new Date(to.getTime() - CONFIG.healingWindowMinutes * 60 * 1000);
    
    // Check last heal time - don't overlap
    const lastHeal = lastHealTime.get(symbol);
    const actualFrom = lastHeal && lastHeal > from ? lastHeal : from;
    
    if (actualFrom >= to) {
        return { symbol, skipped: true, reason: 'already_healed' };
    }
    
    console.log(`🔧 Healing ${symbol}: ${actualFrom.toISOString().slice(11,16)} → ${to.toISOString().slice(11,16)}`);
    
    try {
        // Step 1: Fetch from Dukascopy
        const candles = await fetchM1FromDukascopy(symbol, actualFrom, to);
        
        if (candles.length === 0) {
            console.log(`   ⚠️ No data from Dukascopy`);
            return { symbol, fetched: 0, inserted: 0 };
        }
        
        // Step 2: DELETE existing M1 in this window
        const deleted = await deleteM1Range(symbol, actualFrom, to);
        
        // Step 3: INSERT fresh M1
        const inserted = await insertCandles(candles);
        
        // Step 4: Rebuild higher timeframes
        await rebuildHigherTimeframes(symbol, actualFrom, to);
        
        // Update last heal time
        lastHealTime.set(symbol, to);
        
        console.log(`   ✅ Deleted ${deleted}, inserted ${inserted}`);
        
        return { symbol, deleted, fetched: candles.length, inserted };
        
    } catch (error) {
        console.error(`   ❌ Error: ${error.message}`);
        return { symbol, error: error.message };
    }
}

/**
 * Heal all primary pairs (called every 5 min)
 */
async function healPrimaryPairs() {
    console.log(`\n⏰ [${new Date().toISOString().slice(11,19)}] Healing primary pairs...`);
    
    for (const symbol of CONFIG.primaryPairs) {
        await healSymbol(symbol);
        await sleep(1500); // Rate limit Dukascopy
    }
}

/**
 * Heal all secondary pairs (called every 15 min)
 */
async function healSecondaryPairs() {
    console.log(`\n⏰ [${new Date().toISOString().slice(11,19)}] Healing secondary pairs...`);
    
    for (const symbol of CONFIG.secondaryPairs) {
        await healSymbol(symbol);
        await sleep(1500);
    }
}

// =============================================================================
// DAILY VERIFICATION - Run at 2 AM
// =============================================================================

/**
 * Full verification of previous day's data
 * Uses INSERT IGNORE - fills gaps without overwriting good data
 */
async function dailyVerification() {
    console.log('\n' + '='.repeat(60));
    console.log('🌙 DAILY VERIFICATION - Previous day check');
    console.log('='.repeat(60));
    
    const now = new Date();
    const yesterday = new Date(now);
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);
    yesterday.setUTCHours(0, 0, 0, 0);
    
    const today = new Date(now);
    today.setUTCHours(0, 0, 0, 0);
    
    console.log(`📅 Checking: ${yesterday.toISOString().split('T')[0]}`);
    
    const allPairs = [...CONFIG.primaryPairs, ...CONFIG.secondaryPairs];
    
    for (const symbol of allPairs) {
        console.log(`\n📊 ${symbol}`);
        
        // Check how many M1 candles we have
        const [countResult] = await database.pool.execute(`
            SELECT COUNT(*) as count
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = 'M1'
            AND timestamp >= ? AND timestamp < ?
        `, [symbol, yesterday, today]);
        
        const currentCount = countResult[0].count;
        console.log(`   Current M1 candles: ${currentCount}`);
        
        // Fetch from Dukascopy
        const candles = await fetchM1FromDukascopy(symbol, yesterday, today);
        console.log(`   Dukascopy M1 candles: ${candles.length}`);
        
        if (candles.length === 0) continue;
        
        // INSERT IGNORE - fills gaps without overwriting
        let filled = 0;
        for (const c of candles) {
            try {
                const [result] = await database.pool.execute(`
                    INSERT IGNORE INTO pulse_market_data
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                `, [c.symbol, c.timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume]);
                filled += result.affectedRows;
            } catch (e) {}
        }
        
        if (filled > 0) {
            console.log(`   ✅ Filled ${filled} missing candles`);
            // Rebuild higher TFs
            await rebuildHigherTimeframes(symbol, yesterday, today);
        } else {
            console.log(`   ✅ No gaps found`);
        }
        
        await sleep(2000);
    }
    
    console.log('\n' + '='.repeat(60));
    console.log('✅ Daily verification complete');
    console.log('='.repeat(60));
}

// =============================================================================
// STARTUP & CRON SCHEDULING
// =============================================================================

async function init() {
    console.log('='.repeat(60));
    console.log('🚀 Smart Healing Engine');
    console.log('='.repeat(60));
    console.log(`📊 Primary pairs: ${CONFIG.primaryPairs.length}`);
    console.log(`📊 Secondary pairs: ${CONFIG.secondaryPairs.length}`);
    console.log(`⏱️  Healing window: ${CONFIG.healingWindowMinutes} minutes`);
    console.log(`⏱️  Dukascopy delay: ${CONFIG.dukascopyDelayMinutes} minutes`);
    console.log('='.repeat(60));
    
    await database.connect();
    
    // Schedule cron jobs
    
    // Every 5 minutes: Heal primary pairs
    cron.schedule('*/5 * * * *', () => healPrimaryPairs());
    console.log('✅ Scheduled: Primary pairs healing every 5 minutes');
    
    // Every 15 minutes: Heal secondary pairs
    cron.schedule('*/15 * * * *', () => healSecondaryPairs());
    console.log('✅ Scheduled: Secondary pairs healing every 15 minutes');
    
    // Daily at 2 AM: Full verification
    cron.schedule('0 2 * * *', () => dailyVerification());
    console.log('✅ Scheduled: Daily verification at 02:00 UTC');
    
    console.log('='.repeat(60));
    console.log('🎯 Engine running. First heal in 5 minutes...');
    console.log('   Or run: node smart-healing-engine.js --heal-now');
    console.log('='.repeat(60));
    
    // Check if --heal-now flag
    if (process.argv.includes('--heal-now')) {
        console.log('\n🚀 Running immediate heal...\n');
        await healPrimaryPairs();
        await healSecondaryPairs();
    }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('🛑 Shutting down...');
    await database.disconnect();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('🛑 Shutting down...');
    await database.disconnect();
    process.exit(0);
});

// Export for use by other scripts
module.exports = {
    healSymbol,
    healPrimaryPairs,
    healSecondaryPairs,
    dailyVerification,
    fetchM1FromDukascopy,
    rebuildHigherTimeframes,
    CONFIG,
};

// Run if called directly
if (require.main === module) {
    init().catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}