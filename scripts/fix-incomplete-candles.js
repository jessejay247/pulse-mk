#!/usr/bin/env node
// =============================================================================
// scripts/fix-incomplete-candles.js - Fix Incomplete Candles
// =============================================================================
//
// Detects and fixes candles where OHLC values are identical (incomplete candles
// that were saved before fully forming). Re-fetches correct data from Dukascopy.
//
// Usage:
//   node scripts/fix-incomplete-candles.js              # Check all symbols
//   node scripts/fix-incomplete-candles.js --fix        # Actually fix them
//   node scripts/fix-incomplete-candles.js EURUSD --fix # Fix specific symbol
// =============================================================================

require('dotenv').config();

const database = require('../database');
const dukascopy = require('../services/dukascopy-service');

const CONFIG = {
    defaultDays: 7,
    symbols: [
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
        'XAUUSD', 'XAGUSD',
        'EURGBP', 'EURJPY', 'GBPJPY', 'EURCHF', 'GBPCHF', 'AUDJPY',
        'EURAUD', 'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
        'NZDJPY', 'CADJPY'
    ],
    timeframes: ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1']
};

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Find incomplete candles where OHLC are all the same
 */
async function findIncompleteCandles(symbol, timeframe, fromDate, toDate) {
    const [rows] = await database.pool.execute(`
        SELECT id, timestamp, open, high, low, close, volume
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        AND open = high AND high = low AND low = close
        ORDER BY timestamp ASC
    `, [symbol, timeframe, fromDate, toDate]);

    return rows;
}

/**
 * Find suspicious candles (very small range compared to typical)
 */
async function findSuspiciousCandles(symbol, timeframe, fromDate, toDate) {
    // First get average range for this symbol/timeframe
    const [avgRows] = await database.pool.execute(`
        SELECT AVG(high - low) as avg_range
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        AND high != low
    `, [symbol, timeframe, fromDate, toDate]);

    const avgRange = avgRows[0]?.avg_range || 0;
    if (avgRange === 0) return [];

    // Find candles with range < 10% of average (suspicious)
    const threshold = avgRange * 0.1;

    const [rows] = await database.pool.execute(`
        SELECT id, timestamp, open, high, low, close, volume, (high - low) as range_val
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        AND (high - low) < ?
        AND (high - low) > 0
        ORDER BY timestamp ASC
    `, [symbol, timeframe, fromDate, toDate, threshold]);

    return rows;
}

/**
 * Group timestamps into ranges for efficient fetching
 */
function groupIntoRanges(timestamps, maxGapMinutes = 60) {
    if (timestamps.length === 0) return [];

    const sorted = [...timestamps].sort((a, b) => a - b);
    const ranges = [];
    let rangeStart = sorted[0];
    let rangeEnd = sorted[0];

    for (let i = 1; i < sorted.length; i++) {
        const gap = (sorted[i] - rangeEnd) / (60 * 1000);
        
        if (gap > maxGapMinutes) {
            ranges.push({ from: new Date(rangeStart), to: new Date(rangeEnd) });
            rangeStart = sorted[i];
        }
        rangeEnd = sorted[i];
    }

    ranges.push({ from: new Date(rangeStart), to: new Date(rangeEnd) });
    return ranges;
}

/**
 * Fix incomplete candles by re-fetching from Dukascopy
 */
async function fixIncompleteCandles(symbol, timeframe, incomplete) {
    if (incomplete.length === 0) return 0;

    // Group into date ranges
    const timestamps = incomplete.map(c => new Date(c.timestamp).getTime());
    const ranges = groupIntoRanges(timestamps);

    let fixed = 0;

    for (const range of ranges) {
        // Expand range slightly to ensure we get full candles
        const from = new Date(range.from.getTime() - 60000);
        const to = new Date(range.to.getTime() + 60000);

        try {
            const result = await dukascopy.fetchAndSave(symbol, timeframe, from, to);
            fixed += result;
            console.log(`      ✅ Fixed range ${from.toISOString().split('T')[0]} to ${to.toISOString().split('T')[0]}: ${result} rows`);
        } catch (error) {
            console.log(`      ❌ Error fixing range: ${error.message}`);
        }

        await sleep(500);
    }

    return fixed;
}

async function main() {
    const args = process.argv.slice(2);

    if (args.includes('--help') || args.includes('-h')) {
        console.log(`
🔧 Fix Incomplete Candles

Detects candles where OHLC values are identical (incomplete candles that were
saved before fully forming) and re-fetches correct data from Dukascopy.

Usage:
  node scripts/fix-incomplete-candles.js [options] [symbols...]

Options:
  --fix             Actually fix the incomplete candles
  --days <n>        Number of days to check (default: 7)
  --timeframe <tf>  Specific timeframe only
  --suspicious      Also check for suspiciously small candles
  --help, -h        Show this help

Examples:
  node scripts/fix-incomplete-candles.js                    # Preview all
  node scripts/fix-incomplete-candles.js --fix              # Fix all
  node scripts/fix-incomplete-candles.js EURUSD --fix       # Fix EURUSD only
  node scripts/fix-incomplete-candles.js --days 30 --fix    # Check 30 days
        `);
        process.exit(0);
    }

    let days = CONFIG.defaultDays;
    let fix = false;
    let checkSuspicious = false;
    let symbols = [];
    let timeframes = CONFIG.timeframes;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--days' && args[i + 1]) {
            days = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--fix') {
            fix = true;
        } else if (args[i] === '--suspicious') {
            checkSuspicious = true;
        } else if (args[i] === '--timeframe' && args[i + 1]) {
            timeframes = [args[i + 1].toUpperCase()];
            i++;
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }

    if (symbols.length === 0) {
        symbols = CONFIG.symbols;
    }

    await database.connect();

    const toDate = new Date();
    const fromDate = new Date(toDate.getTime() - days * 24 * 60 * 60 * 1000);

    console.log('='.repeat(60));
    console.log('🔧 Fix Incomplete Candles');
    console.log('='.repeat(60));
    console.log(`📅 Checking: ${fromDate.toISOString().split('T')[0]} to ${toDate.toISOString().split('T')[0]}`);
    console.log(`💱 Symbols: ${symbols.length}`);
    console.log(`📊 Timeframes: ${timeframes.join(', ')}`);
    console.log(`🔧 Mode: ${fix ? '⚠️  FIX (will update!)' : '👀 Preview only'}`);
    console.log('='.repeat(60));

    let totalIncomplete = 0;
    let totalFixed = 0;
    const report = [];

    for (const symbol of symbols) {
        console.log(`\n💱 ${symbol}`);
        console.log('─'.repeat(40));

        for (const timeframe of timeframes) {
            // Find incomplete candles
            const incomplete = await findIncompleteCandles(symbol, timeframe, fromDate, toDate);
            
            if (incomplete.length > 0) {
                console.log(`   ${timeframe}: ⚠️  ${incomplete.length} incomplete candles`);
                totalIncomplete += incomplete.length;

                // Show samples
                const samples = incomplete.slice(0, 3);
                for (const s of samples) {
                    console.log(`      ${new Date(s.timestamp).toISOString()} - all OHLC = ${s.close}`);
                }
                if (incomplete.length > 3) {
                    console.log(`      ... and ${incomplete.length - 3} more`);
                }

                report.push({
                    symbol,
                    timeframe,
                    count: incomplete.length,
                    candles: incomplete
                });

                if (fix) {
                    const fixed = await fixIncompleteCandles(symbol, timeframe, incomplete);
                    totalFixed += fixed;
                }
            } else {
                console.log(`   ${timeframe}: ✅ No incomplete candles`);
            }

            // Optionally check suspicious candles
            if (checkSuspicious) {
                const suspicious = await findSuspiciousCandles(symbol, timeframe, fromDate, toDate);
                if (suspicious.length > 0) {
                    console.log(`   ${timeframe}: 🔍 ${suspicious.length} suspicious (very small range)`);
                }
            }
        }
    }

    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total incomplete candles found: ${totalIncomplete}`);
    
    if (fix) {
        console.log(`Total rows processed: ${totalFixed}`);
    } else if (totalIncomplete > 0) {
        console.log('\n💡 Run with --fix to update incomplete candles with correct data');
    }

    if (report.length > 0) {
        console.log('\n📋 Breakdown:');
        for (const r of report) {
            console.log(`   ${r.symbol} ${r.timeframe}: ${r.count} incomplete`);
        }
    }

    console.log('='.repeat(60));

    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Error:', error);
    process.exit(1);
});

module.exports = { findIncompleteCandles, fixIncompleteCandles };