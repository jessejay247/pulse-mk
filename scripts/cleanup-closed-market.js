#!/usr/bin/env node
// =============================================================================
// scripts/cleanup-closed-market.js - Remove Candles Added During Closed Market
// =============================================================================
//
// This script removes forex/metal candles that were incorrectly saved during
// market closed hours (weekends, holidays).
//
// Usage:
//   node scripts/cleanup-closed-market.js              # Dry run (preview)
//   node scripts/cleanup-closed-market.js --execute    # Actually delete
//   node scripts/cleanup-closed-market.js --days 30    # Check last 30 days
//   node scripts/cleanup-closed-market.js EURUSD       # Specific symbol only
// =============================================================================

require('dotenv').config();

const database = require('../database');

// Forex market hours: Sunday 21:00 UTC to Friday 22:00 UTC
// Crypto: 24/7 (no cleanup needed)
// Stocks: Weekdays only (handled separately)

const CONFIG = {
    defaultDays: 14,
    forexSymbols: [
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
        'EURGBP', 'EURJPY', 'GBPJPY', 'EURCHF', 'GBPCHF', 'AUDJPY',
        'EURAUD', 'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
        'NZDJPY', 'CADJPY'
    ],
    metalSymbols: ['XAUUSD', 'XAGUSD'],
    // Crypto symbols are 24/7, no cleanup needed
};

/**
 * Check if a timestamp is during forex market closed hours
 */
function isMarketClosed(timestamp) {
    const date = new Date(timestamp);
    const day = date.getUTCDay();
    const hour = date.getUTCHours();
    const minute = date.getUTCMinutes();
    const timeValue = hour * 60 + minute;

    // Saturday: Always closed
    if (day === 6) {
        return { closed: true, reason: 'Saturday' };
    }

    // Sunday: Closed before 21:00 UTC
    if (day === 0) {
        if (timeValue < 21 * 60) {
            return { closed: true, reason: 'Sunday before 21:00 UTC' };
        }
        return { closed: false };
    }

    // Friday: Closed after 22:00 UTC
    if (day === 5) {
        if (timeValue >= 22 * 60) {
            return { closed: true, reason: 'Friday after 22:00 UTC' };
        }
    }

    // Monday - Thursday: Always open (for forex)
    return { closed: false };
}

/**
 * Find candles that were saved during closed market hours
 */
async function findInvalidCandles(symbol, fromDate, toDate) {
    const [rows] = await database.pool.execute(`
        SELECT id, symbol, timeframe, timestamp
        FROM pulse_market_data
        WHERE symbol = ?
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    `, [symbol, fromDate, toDate]);

    const invalidCandles = [];

    for (const row of rows) {
        const check = isMarketClosed(row.timestamp);
        if (check.closed) {
            invalidCandles.push({
                id: row.id,
                symbol: row.symbol,
                timeframe: row.timeframe,
                timestamp: row.timestamp,
                reason: check.reason
            });
        }
    }

    return invalidCandles;
}

/**
 * Delete invalid candles by IDs
 */
async function deleteCandles(ids) {
    if (ids.length === 0) return 0;

    // Delete in batches of 1000
    let deleted = 0;
    for (let i = 0; i < ids.length; i += 1000) {
        const batch = ids.slice(i, i + 1000);
        const placeholders = batch.map(() => '?').join(',');
        
        const [result] = await database.pool.execute(`
            DELETE FROM pulse_market_data WHERE id IN (${placeholders})
        `, batch);
        
        deleted += result.affectedRows;
    }

    return deleted;
}

/**
 * Main cleanup function
 */
async function cleanup(options = {}) {
    const { days = CONFIG.defaultDays, symbols = null, execute = false } = options;

    await database.connect();

    const toDate = new Date();
    const fromDate = new Date(toDate.getTime() - days * 24 * 60 * 60 * 1000);

    const targetSymbols = symbols || [...CONFIG.forexSymbols, ...CONFIG.metalSymbols];

    console.log('='.repeat(60));
    console.log('🧹 Cleanup Closed Market Candles');
    console.log('='.repeat(60));
    console.log(`📅 Checking: ${fromDate.toISOString().split('T')[0]} to ${toDate.toISOString().split('T')[0]}`);
    console.log(`💱 Symbols: ${targetSymbols.length}`);
    console.log(`🔧 Mode: ${execute ? '⚠️  EXECUTE (will delete!)' : '👀 DRY RUN (preview only)'}`);
    console.log('='.repeat(60));

    let totalInvalid = 0;
    let totalDeleted = 0;
    const report = [];

    for (const symbol of targetSymbols) {
        const invalid = await findInvalidCandles(symbol, fromDate, toDate);
        
        if (invalid.length > 0) {
            console.log(`\n❌ ${symbol}: ${invalid.length} invalid candles found`);
            
            // Group by reason for summary
            const byReason = {};
            const byTimeframe = {};
            
            for (const candle of invalid) {
                byReason[candle.reason] = (byReason[candle.reason] || 0) + 1;
                byTimeframe[candle.timeframe] = (byTimeframe[candle.timeframe] || 0) + 1;
            }

            console.log('   By reason:', Object.entries(byReason).map(([r, c]) => `${r}: ${c}`).join(', '));
            console.log('   By timeframe:', Object.entries(byTimeframe).map(([t, c]) => `${t}: ${c}`).join(', '));

            // Show sample timestamps
            const samples = invalid.slice(0, 3);
            for (const s of samples) {
                const ts = new Date(s.timestamp);
                console.log(`   Sample: ${s.timeframe} @ ${ts.toISOString()} (${s.reason})`);
            }
            if (invalid.length > 3) {
                console.log(`   ... and ${invalid.length - 3} more`);
            }

            report.push({
                symbol,
                count: invalid.length,
                byReason,
                byTimeframe,
                ids: invalid.map(c => c.id)
            });

            totalInvalid += invalid.length;

            if (execute) {
                const deleted = await deleteCandles(invalid.map(c => c.id));
                totalDeleted += deleted;
                console.log(`   ✅ Deleted ${deleted} candles`);
            }
        } else {
            console.log(`✅ ${symbol}: No invalid candles`);
        }
    }

    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total invalid candles found: ${totalInvalid}`);
    
    if (execute) {
        console.log(`Total candles deleted: ${totalDeleted}`);
    } else if (totalInvalid > 0) {
        console.log('\n💡 Run with --execute to actually delete these candles');
        console.log('   Example: node scripts/cleanup-closed-market.js --execute');
    }

    // Breakdown by day of week
    if (report.length > 0) {
        console.log('\n📅 Breakdown by reason:');
        const totalByReason = {};
        for (const r of report) {
            for (const [reason, count] of Object.entries(r.byReason)) {
                totalByReason[reason] = (totalByReason[reason] || 0) + count;
            }
        }
        for (const [reason, count] of Object.entries(totalByReason)) {
            console.log(`   ${reason}: ${count} candles`);
        }
    }

    console.log('='.repeat(60));

    await database.disconnect();

    return { totalInvalid, totalDeleted, report };
}

// CLI
async function main() {
    const args = process.argv.slice(2);

    if (args.includes('--help') || args.includes('-h')) {
        console.log(`
🧹 Cleanup Closed Market Candles

This script removes forex/metal candles that were incorrectly saved during
closed market hours (weekends: Saturday all day, Sunday before 21:00 UTC,
Friday after 22:00 UTC).

Usage:
  node scripts/cleanup-closed-market.js [options] [symbols...]

Options:
  --execute       Actually delete the candles (default is dry run)
  --days <n>      Number of days to check (default: 14)
  --help, -h      Show this help

Examples:
  node scripts/cleanup-closed-market.js                    # Preview all symbols
  node scripts/cleanup-closed-market.js --execute          # Delete all invalid
  node scripts/cleanup-closed-market.js EURUSD GBPUSD      # Check specific symbols
  node scripts/cleanup-closed-market.js --days 30 --execute # Check 30 days & delete

Note: Crypto symbols are 24/7 and not affected by this cleanup.
        `);
        process.exit(0);
    }

    let days = CONFIG.defaultDays;
    let execute = false;
    const symbols = [];

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--days' && args[i + 1]) {
            days = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--execute') {
            execute = true;
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }

    await cleanup({
        days,
        execute,
        symbols: symbols.length > 0 ? symbols : null
    });
}

main().catch(error => {
    console.error('❌ Error:', error);
    process.exit(1);
});

module.exports = { cleanup, isMarketClosed, findInvalidCandles };