#!/usr/bin/env node
// =============================================================================
// scripts/nuclear-fix.js - DELETE ALL + Re-fetch from Dukascopy
// =============================================================================
//
// RATE-LIMITED VERSION:
// - Respects Dukascopy's rate limits (~20 req/min safe)
// - 3 second delay between requests
// - Automatic retry on rate limit errors
// - Safety buffer to preserve recent real-time candles
//
// Usage:
//   node scripts/nuclear-fix.js EURUSD --hours 2
//   node scripts/nuclear-fix.js --primary --days 1
// =============================================================================

require('dotenv').config();

const { getHistoricalRates } = require('dukascopy-node');
const database = require('../database');

// =============================================================================
// CONFIGURATION
// =============================================================================

const PRIMARY_PAIRS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF',
    'AUDUSD', 'USDCAD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
];

const ALL_PAIRS = [
    ...PRIMARY_PAIRS,
    'XAGUSD', 'EURCHF', 'GBPCHF', 'AUDJPY', 'EURAUD',
    'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
    'NZDJPY', 'CADJPY'
];

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

const ALL_TIMEFRAMES = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'];

// Rate limiting config
const RATE_LIMIT = {
    delayBetweenRequests: 3000,    // 3 seconds = ~20 req/min
    delayBetweenSymbols: 5000,     // 5 seconds between symbols
    retryDelay: 60000,              // 1 minute on rate limit error
    maxRetries: 2,
};

// Safety buffer - Dukascopy has ~30-45 min delay
const SAFETY_BUFFER_MINUTES = 30;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// =============================================================================
// DUKASCOPY FETCH WITH RETRY
// =============================================================================

async function fetchFromDukascopy(symbol, timeframe, from, to, attempt = 1) {
    const instrument = DUKASCOPY_INSTRUMENTS[symbol];
    if (!instrument) {
        return { candles: [], error: 'unsupported' };
    }
    
    const tfMap = { 'M1': 'm1', 'M5': 'm5', 'M15': 'm15', 'M30': 'm30', 'H1': 'h1', 'H4': 'h4', 'D1': 'd1' };
    const tf = tfMap[timeframe];
    
    try {
        const data = await getHistoricalRates({
            instrument,
            dates: { from, to },
            timeframe: tf,
            format: 'json',
            priceType: 'bid',
            volumes: true,
        });
        
        const candles = data.map(candle => ({
            symbol,
            timeframe,
            timestamp: new Date(candle.timestamp),
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume || 0,
        }));
        
        return { candles, error: null };
        
    } catch (error) {
        const msg = error.message || 'unknown';
        
        // Check for rate limit
        if (msg.includes('429') || msg.toLowerCase().includes('rate') || msg.toLowerCase().includes('limit')) {
            if (attempt < RATE_LIMIT.maxRetries) {
                console.log(`   ⚠️ Rate limited, waiting ${RATE_LIMIT.retryDelay/1000}s before retry...`);
                await sleep(RATE_LIMIT.retryDelay);
                return fetchFromDukascopy(symbol, timeframe, from, to, attempt + 1);
            }
            return { candles: [], error: 'rate_limited' };
        }
        
        // Network errors - retry once
        if ((msg.includes('ECONNRESET') || msg.includes('ETIMEDOUT')) && attempt < 2) {
            console.log(`   ⚠️ Network error, retrying in 5s...`);
            await sleep(5000);
            return fetchFromDukascopy(symbol, timeframe, from, to, attempt + 1);
        }
        
        return { candles: [], error: msg };
    }
}

// =============================================================================
// DATABASE OPERATIONS
// =============================================================================

async function deleteAllCandles(symbol, timeframe, from, to) {
    const [result] = await database.pool.execute(`
        DELETE FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp >= ? AND timestamp < ?
    `, [symbol, timeframe, from, to]);
    
    return result.affectedRows;
}

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
            // Individual insert fallback
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

// =============================================================================
// NUCLEAR FIX - MAIN FUNCTION
// =============================================================================

async function nuclearFix(symbol, from, to) {
    console.log(`\n💣 NUCLEAR FIX: ${symbol}`);
    console.log(`   📅 ${from.toISOString()} → ${to.toISOString()}`);
    
    const stats = { deleted: 0, inserted: 0, errors: 0 };
    
    for (const tf of ALL_TIMEFRAMES) {
        process.stdout.write(`   ${tf}: `);
        
        // Fetch from Dukascopy
        const result = await fetchFromDukascopy(symbol, tf, from, to);
        
        if (result.error) {
            console.log(`❌ ${result.error}`);
            stats.errors++;
            await sleep(RATE_LIMIT.delayBetweenRequests);
            continue;
        }
        
        if (result.candles.length === 0) {
            console.log(`no data`);
            await sleep(RATE_LIMIT.delayBetweenRequests);
            continue;
        }
        
        // Delete existing
        const deleted = await deleteAllCandles(symbol, tf, from, to);
        stats.deleted += deleted;
        
        // Insert fresh
        const inserted = await insertCandles(result.candles);
        stats.inserted += inserted;
        
        console.log(`deleted ${deleted}, inserted ${inserted} ✅`);
        
        // Rate limit between timeframes
        await sleep(RATE_LIMIT.delayBetweenRequests);
    }
    
    console.log(`   📊 Total: deleted ${stats.deleted}, inserted ${stats.inserted}, errors ${stats.errors}`);
    return stats;
}

// =============================================================================
// CLI
// =============================================================================

async function main() {
    const args = process.argv.slice(2);
    
    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        console.log(`
💣 NUCLEAR FIX - DELETE ALL + Re-fetch from Dukascopy

Rate-limited version - safe for continuous use.

Usage:
  node scripts/nuclear-fix.js <symbols...> [options]

Options:
  --hours <n>    Hours to fix (default: 24)
  --days <n>     Days to fix
  --primary      Fix all primary pairs
  --all          Fix all pairs

Examples:
  node scripts/nuclear-fix.js EURUSD --hours 2
  node scripts/nuclear-fix.js EURUSD GBPUSD --days 1
  node scripts/nuclear-fix.js --primary --hours 6

Rate Limits:
  - ${RATE_LIMIT.delayBetweenRequests/1000}s between requests (~${Math.floor(60000/RATE_LIMIT.delayBetweenRequests)}/min)
  - ${RATE_LIMIT.delayBetweenSymbols/1000}s between symbols
  - Auto-retry on rate limit with ${RATE_LIMIT.retryDelay/1000}s wait

⚠️  Safety: Preserves last ${SAFETY_BUFFER_MINUTES} min (Dukascopy delay)
        `);
        return;
    }
    
    // Parse arguments
    let hours = 24;
    let symbols = [];
    
    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--hours' && args[i + 1]) {
            hours = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--days' && args[i + 1]) {
            hours = parseInt(args[i + 1]) * 24;
            i++;
        } else if (args[i] === '--primary') {
            symbols = [...PRIMARY_PAIRS];
        } else if (args[i] === '--all') {
            symbols = [...ALL_PAIRS];
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }
    
    if (symbols.length === 0) {
        console.error('❌ No symbols specified. Use --primary, --all, or provide symbol names.');
        return;
    }
    
    // Calculate time range with safety buffer
    const now = new Date();
    const safetyBuffer = SAFETY_BUFFER_MINUTES * 60 * 1000;
    const to = new Date(now.getTime() - safetyBuffer);
    const from = new Date(to.getTime() - hours * 60 * 60 * 1000);
    
    // Estimate time
    const totalRequests = symbols.length * ALL_TIMEFRAMES.length;
    const estimatedMinutes = Math.ceil((totalRequests * RATE_LIMIT.delayBetweenRequests + symbols.length * RATE_LIMIT.delayBetweenSymbols) / 60000);
    
    console.log('═'.repeat(60));
    console.log('💣 NUCLEAR FIX - Rate-Limited Dukascopy Repair');
    console.log('═'.repeat(60));
    console.log(`📅 Period: Last ${hours} hours`);
    console.log(`📅 From: ${from.toISOString()}`);
    console.log(`📅 To:   ${to.toISOString()}`);
    console.log(`🛡️  Safety buffer: ${SAFETY_BUFFER_MINUTES} minutes`);
    console.log(`💱 Symbols: ${symbols.join(', ')}`);
    console.log(`📊 Timeframes: ${ALL_TIMEFRAMES.join(', ')}`);
    console.log(`⏱️  Estimated time: ~${estimatedMinutes} minutes`);
    console.log('═'.repeat(60));
    
    await database.connect();
    
    let totalDeleted = 0;
    let totalInserted = 0;
    let totalErrors = 0;
    
    for (let i = 0; i < symbols.length; i++) {
        const symbol = symbols[i];
        console.log(`\n[${i+1}/${symbols.length}] Processing ${symbol}...`);
        
        const stats = await nuclearFix(symbol, from, to);
        totalDeleted += stats.deleted;
        totalInserted += stats.inserted;
        totalErrors += stats.errors;
        
        // Delay between symbols
        if (i < symbols.length - 1) {
            console.log(`   ⏳ Waiting ${RATE_LIMIT.delayBetweenSymbols/1000}s before next symbol...`);
            await sleep(RATE_LIMIT.delayBetweenSymbols);
        }
    }
    
    console.log('\n' + '═'.repeat(60));
    console.log('📊 FINAL SUMMARY');
    console.log('═'.repeat(60));
    console.log(`Total deleted:  ${totalDeleted}`);
    console.log(`Total inserted: ${totalInserted}`);
    console.log(`Total errors:   ${totalErrors}`);
    console.log('═'.repeat(60));
    console.log('✅ Done! Refresh your app to see the fixed data.');
    
    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});