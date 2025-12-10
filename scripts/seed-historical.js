#!/usr/bin/env node
// =============================================================================
// scripts/seed-historical.js - Historical Data Seeding (INSERT IGNORE)
// =============================================================================
//
// This script ADDS historical data without overwriting existing data.
// Uses INSERT IGNORE - if a candle already exists, it's skipped.
//
// Use Cases:
// - Initial data population
// - Extending history backwards (1 month → 3 months)
// - Filling gaps in historical data
//
// Usage:
//   node scripts/seed-historical.js EURUSD --months 1
//   node scripts/seed-historical.js --primary --months 1
//   node scripts/seed-historical.js --secondary --months 3
//   node scripts/seed-historical.js --all --months 1
//   node scripts/seed-historical.js --all --preset quick    # Recommended durations
//   node scripts/seed-historical.js --all --preset full     # Maximum history
//
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

const SECONDARY_PAIRS = [
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

// Recommended durations per timeframe
const PRESETS = {
    // Quick start - minimal but useful
    quick: {
        M1: { months: 1 },      // ~43,200 candles/pair
        M5: { months: 3 },      // ~25,920 candles/pair
        M15: { months: 6 },     // ~17,280 candles/pair
        M30: { months: 12 },    // ~17,280 candles/pair
        H1: { years: 2 },       // ~17,520 candles/pair
        H4: { years: 5 },       // ~10,950 candles/pair
        D1: { years: 10 },      // ~3,650 candles/pair
    },
    // Full history - maximum data
    full: {
        M1: { months: 3 },      // ~129,600 candles/pair
        M5: { months: 12 },     // ~103,680 candles/pair
        M15: { years: 2 },      // ~69,120 candles/pair
        M30: { years: 3 },      // ~52,560 candles/pair
        H1: { years: 5 },       // ~43,800 candles/pair
        H4: { years: 10 },      // ~21,900 candles/pair
        D1: { years: 20 },      // ~7,300 candles/pair
    },
    // Minimal - for testing
    minimal: {
        M1: { days: 7 },
        M5: { days: 14 },
        M15: { months: 1 },
        M30: { months: 1 },
        H1: { months: 3 },
        H4: { months: 6 },
        D1: { years: 1 },
    },
};

// Chunk sizes (days per request) to avoid memory issues
const CHUNK_SIZES = {
    M1: 1,    // 1 day at a time
    M5: 7,    // 1 week
    M15: 14,  // 2 weeks
    M30: 30,  // 1 month
    H1: 60,   // 2 months
    H4: 180,  // 6 months
    D1: 365,  // 1 year
};

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function calculateFromDate(duration) {
    const now = new Date();
    if (duration.days) {
        now.setDate(now.getDate() - duration.days);
    } else if (duration.months) {
        now.setMonth(now.getMonth() - duration.months);
    } else if (duration.years) {
        now.setFullYear(now.getFullYear() - duration.years);
    }
    return now;
}

function splitIntoChunks(from, to, timeframe) {
    const chunkDays = CHUNK_SIZES[timeframe] || 7;
    const chunkMs = chunkDays * 24 * 60 * 60 * 1000;
    const chunks = [];
    
    let currentStart = new Date(from);
    while (currentStart < to) {
        const chunkEnd = new Date(Math.min(currentStart.getTime() + chunkMs, to.getTime()));
        chunks.push({ from: new Date(currentStart), to: chunkEnd });
        currentStart = new Date(chunkEnd);
    }
    
    return chunks;
}

// =============================================================================
// CORE FUNCTIONS
// =============================================================================

/**
 * Fetch candles from Dukascopy
 */
async function fetchFromDukascopy(symbol, timeframe, from, to) {
    const instrument = DUKASCOPY_INSTRUMENTS[symbol];
    if (!instrument) return [];
    
    const tfMap = { M1: 'm1', M5: 'm5', M15: 'm15', M30: 'm30', H1: 'h1', H4: 'h4', D1: 'd1' };
    
    try {
        const data = await getHistoricalRates({
            instrument,
            dates: { from, to },
            timeframe: tfMap[timeframe],
            format: 'json',
            priceType: 'bid',
            volumes: true,
        });
        
        return data.map(c => ({
            symbol,
            timeframe,
            timestamp: new Date(c.timestamp),
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume || 0,
        }));
    } catch (error) {
        console.error(`   ❌ Error: ${error.message}`);
        return [];
    }
}

/**
 * INSERT IGNORE - adds missing candles, skips existing
 */
async function insertIgnoreCandles(candles) {
    if (candles.length === 0) return { inserted: 0, skipped: 0 };
    
    const batchSize = 500;
    let inserted = 0;
    
    for (let i = 0; i < candles.length; i += batchSize) {
        const batch = candles.slice(i, i + batchSize);
        
        for (const c of batch) {
            try {
                const [result] = await database.pool.execute(`
                    INSERT IGNORE INTO pulse_market_data
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                `, [c.symbol, c.timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume]);
                
                inserted += result.affectedRows;
            } catch (e) {
                // Skip errors
            }
        }
    }
    
    return { inserted, skipped: candles.length - inserted };
}

/**
 * Seed a single symbol/timeframe combination
 */
async function seedTimeframe(symbol, timeframe, from, to) {
    const chunks = splitIntoChunks(from, to, timeframe);
    let totalInserted = 0;
    let totalSkipped = 0;
    let totalFetched = 0;
    
    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const progress = Math.round((i / chunks.length) * 100);
        
        process.stdout.write(`\r   ${timeframe}: ${progress}% - ${chunk.from.toISOString().split('T')[0]}...`);
        
        const candles = await fetchFromDukascopy(symbol, timeframe, chunk.from, chunk.to);
        totalFetched += candles.length;
        
        if (candles.length > 0) {
            const { inserted, skipped } = await insertIgnoreCandles(candles);
            totalInserted += inserted;
            totalSkipped += skipped;
        }
        
        await sleep(1000); // Rate limit
    }
    
    console.log(`\r   ${timeframe}: ✅ Fetched ${totalFetched}, inserted ${totalInserted}, skipped ${totalSkipped} (existing)`);
    
    return { fetched: totalFetched, inserted: totalInserted, skipped: totalSkipped };
}

/**
 * Seed all timeframes for a symbol
 */
async function seedSymbol(symbol, options = {}) {
    const { preset, months, years, days, timeframes } = options;
    
    console.log(`\n💱 ${symbol}`);
    console.log('─'.repeat(50));
    
    const targetTimeframes = timeframes || ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'];
    const stats = { fetched: 0, inserted: 0, skipped: 0 };
    
    for (const tf of targetTimeframes) {
        // Determine duration
        let duration;
        if (preset && PRESETS[preset]) {
            duration = PRESETS[preset][tf];
        } else if (months) {
            duration = { months };
        } else if (years) {
            duration = { years };
        } else if (days) {
            duration = { days };
        } else {
            duration = PRESETS.quick[tf]; // Default to quick preset
        }
        
        const from = calculateFromDate(duration);
        const to = new Date();
        
        const result = await seedTimeframe(symbol, tf, from, to);
        stats.fetched += result.fetched;
        stats.inserted += result.inserted;
        stats.skipped += result.skipped;
    }
    
    console.log(`   📊 Total: ${stats.inserted} new, ${stats.skipped} existing`);
    return stats;
}

// =============================================================================
// CLI
// =============================================================================

async function main() {
    const args = process.argv.slice(2);
    
    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        console.log(`
📊 Historical Data Seeding (INSERT IGNORE)

This script ADDS historical data without overwriting existing data.
Safe to run multiple times - existing candles are preserved.

Usage:
  node scripts/seed-historical.js <symbols/flags> [options]

Symbol Selection:
  EURUSD GBPUSD ...     Specific symbols
  --primary             All primary pairs (${PRIMARY_PAIRS.length})
  --secondary           All secondary pairs (${SECONDARY_PAIRS.length})
  --all                 All pairs

Duration Options:
  --days <n>            Number of days
  --months <n>          Number of months
  --years <n>           Number of years
  --preset <name>       Use preset durations (quick, full, minimal)

Timeframe Filter:
  --timeframe M1        Only seed M1
  --timeframe M1,M5,H1  Multiple timeframes

Presets:
  quick    - Recommended starting point (default)
             M1: 1 month, M5: 3 months, H1: 2 years, D1: 10 years
  full     - Maximum history
             M1: 3 months, M5: 12 months, H1: 5 years, D1: 20 years
  minimal  - For testing
             M1: 7 days, M5: 14 days, H1: 3 months, D1: 1 year

Examples:
  # Single pair
  node scripts/seed-historical.js EURUSD --months 1
  node scripts/seed-historical.js EURUSD --preset quick

  # Primary pairs
  node scripts/seed-historical.js --primary --months 1
  node scripts/seed-historical.js --primary --preset quick

  # Secondary pairs
  node scripts/seed-historical.js --secondary --months 3

  # All pairs
  node scripts/seed-historical.js --all --preset quick
  node scripts/seed-historical.js --all --preset full

  # Specific timeframe only
  node scripts/seed-historical.js EURUSD --timeframe M1 --months 1
  node scripts/seed-historical.js --primary --timeframe D1 --years 10

💡 Recommended first run:
   node scripts/seed-historical.js --primary --preset quick
        `);
        return;
    }
    
    // Parse arguments
    let symbols = [];
    let months = null;
    let years = null;
    let days = null;
    let preset = null;
    let timeframes = null;
    
    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--months' && args[i + 1]) {
            months = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--years' && args[i + 1]) {
            years = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--days' && args[i + 1]) {
            days = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--preset' && args[i + 1]) {
            preset = args[i + 1];
            i++;
        } else if (args[i] === '--timeframe' && args[i + 1]) {
            timeframes = args[i + 1].toUpperCase().split(',');
            i++;
        } else if (args[i] === '--primary') {
            symbols = [...symbols, ...PRIMARY_PAIRS];
        } else if (args[i] === '--secondary') {
            symbols = [...symbols, ...SECONDARY_PAIRS];
        } else if (args[i] === '--all') {
            symbols = [...PRIMARY_PAIRS, ...SECONDARY_PAIRS];
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }
    
    // Remove duplicates
    symbols = [...new Set(symbols)];
    
    if (symbols.length === 0) {
        console.error('❌ No symbols specified. Use --primary, --secondary, --all, or symbol names.');
        return;
    }
    
    // Default to quick preset if no duration specified
    if (!months && !years && !days && !preset) {
        preset = 'quick';
    }
    
    console.log('='.repeat(60));
    console.log('📊 Historical Data Seeding');
    console.log('='.repeat(60));
    console.log(`💱 Symbols: ${symbols.length}`);
    console.log(`📈 Timeframes: ${timeframes ? timeframes.join(', ') : 'All'}`);
    if (preset) console.log(`📦 Preset: ${preset}`);
    if (months) console.log(`📅 Duration: ${months} months`);
    if (years) console.log(`📅 Duration: ${years} years`);
    if (days) console.log(`📅 Duration: ${days} days`);
    console.log(`💾 Mode: INSERT IGNORE (preserves existing data)`);
    console.log('='.repeat(60));
    
    await database.connect();
    
    const totalStats = { fetched: 0, inserted: 0, skipped: 0 };
    const startTime = Date.now();
    
    for (const symbol of symbols) {
        const stats = await seedSymbol(symbol, { preset, months, years, days, timeframes });
        totalStats.fetched += stats.fetched;
        totalStats.inserted += stats.inserted;
        totalStats.skipped += stats.skipped;
    }
    
    const duration = Math.round((Date.now() - startTime) / 1000);
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 FINAL SUMMARY');
    console.log('='.repeat(60));
    console.log(`⏱️  Duration: ${Math.floor(duration / 60)}m ${duration % 60}s`);
    console.log(`📥 Total fetched: ${totalStats.fetched.toLocaleString()}`);
    console.log(`✅ New candles inserted: ${totalStats.inserted.toLocaleString()}`);
    console.log(`⏭️  Existing (skipped): ${totalStats.skipped.toLocaleString()}`);
    console.log('='.repeat(60));
    
    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});