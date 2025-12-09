#!/usr/bin/env node
// =============================================================================
// scripts/backfill-gaps.js - Manual Gap Detection and Backfill
// =============================================================================
// 
// Use this script to:
// - Detect missing candles in your data
// - Backfill gaps from Dukascopy
// - Verify data integrity
//
// Usage:
//   node scripts/backfill-gaps.js                    # Check all symbols, last 7 days
//   node scripts/backfill-gaps.js EURUSD            # Check specific symbol
//   node scripts/backfill-gaps.js --days 30         # Check last 30 days
//   node scripts/backfill-gaps.js --fix             # Auto-fix gaps
//   node scripts/backfill-gaps.js EURUSD --days 30 --fix
// =============================================================================

require('dotenv').config();

const database = require('../database');
const dukascopy = require('../services/dukascopy-service');

// Configuration
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
 * Check data coverage and gaps
 */
async function checkDataCoverage(symbol, timeframe, from, to) {
    const [rows] = await database.pool.execute(`
        SELECT 
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(*) as count
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
    `, [symbol, timeframe, from, to]);

    return {
        earliest: rows[0].earliest,
        latest: rows[0].latest,
        count: rows[0].count || 0
    };
}

/**
 * Detect gaps in data
 */
async function detectGaps(symbol, timeframe, from, to) {
    const gaps = await dukascopy.detectGaps(symbol, timeframe, from, to);
    return gaps;
}

/**
 * Fix gaps by fetching from Dukascopy
 */
async function fixGaps(symbol, timeframe, gaps) {
    let totalFixed = 0;

    for (const gap of gaps) {
        console.log(`   🔧 Fixing gap: ${gap.from.toISOString()} to ${gap.to.toISOString()}`);
        
        try {
            const inserted = await dukascopy.fetchAndSave(symbol, timeframe, gap.from, gap.to);
            totalFixed += inserted;
            console.log(`      ✅ Inserted ${inserted} candles`);
        } catch (error) {
            console.log(`      ❌ Error: ${error.message}`);
        }
        
        await sleep(500);
    }

    return totalFixed;
}

/**
 * Main function
 */
async function main() {
    const args = process.argv.slice(2);
    
    // Parse arguments
    let symbols = [];
    let days = CONFIG.defaultDays;
    let autoFix = false;
    let timeframes = CONFIG.timeframes;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--days' && args[i + 1]) {
            days = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--fix') {
            autoFix = true;
        } else if (args[i] === '--timeframe' && args[i + 1]) {
            timeframes = [args[i + 1].toUpperCase()];
            i++;
        } else if (args[i] === '--help' || args[i] === '-h') {
            console.log(`
📊 PulseMarkets Gap Detection & Backfill Tool

Usage:
  node scripts/backfill-gaps.js [options] [symbols...]

Options:
  --days <n>        Number of days to check (default: 7)
  --timeframe <tf>  Specific timeframe to check
  --fix             Automatically fix detected gaps
  --help, -h        Show this help

Examples:
  node scripts/backfill-gaps.js                     # Check all, last 7 days
  node scripts/backfill-gaps.js EURUSD GBPUSD      # Check specific symbols
  node scripts/backfill-gaps.js --days 30          # Check last 30 days
  node scripts/backfill-gaps.js --fix              # Auto-fix all gaps
  node scripts/backfill-gaps.js EURUSD --days 30 --fix
            `);
            process.exit(0);
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }

    if (symbols.length === 0) {
        symbols = CONFIG.symbols;
    }

    // Connect to database
    await database.connect();

    console.log('='.repeat(60));
    console.log('📊 PulseMarkets Gap Detection & Backfill');
    console.log('='.repeat(60));
    console.log(`📅 Checking last ${days} days`);
    console.log(`💱 Symbols: ${symbols.length}`);
    console.log(`📈 Timeframes: ${timeframes.join(', ')}`);
    console.log(`🔧 Auto-fix: ${autoFix ? 'Yes' : 'No'}`);
    console.log('='.repeat(60));

    const now = new Date();
    const fromDate = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

    let totalGaps = 0;
    let totalFixed = 0;
    const gapReport = [];

    for (const symbol of symbols) {
        console.log(`\n💱 ${symbol}`);
        console.log('─'.repeat(40));

        for (const timeframe of timeframes) {
            // Check coverage
            const coverage = await checkDataCoverage(symbol, timeframe, fromDate, now);
            
            if (coverage.count === 0) {
                console.log(`   ${timeframe}: ❌ No data`);
                
                if (autoFix) {
                    console.log(`      🔧 Fetching data...`);
                    const inserted = await dukascopy.fetchAndSave(symbol, timeframe, fromDate, now);
                    console.log(`      ✅ Inserted ${inserted} candles`);
                    totalFixed += inserted;
                }
                continue;
            }

            // Detect gaps
            const gaps = await detectGaps(symbol, timeframe, fromDate, now);
            
            if (gaps.length > 0) {
                console.log(`   ${timeframe}: ⚠️  ${coverage.count} candles, ${gaps.length} gaps`);
                totalGaps += gaps.length;

                gapReport.push({
                    symbol,
                    timeframe,
                    gaps: gaps.length,
                    details: gaps
                });

                if (autoFix) {
                    const fixed = await fixGaps(symbol, timeframe, gaps);
                    totalFixed += fixed;
                }
            } else {
                console.log(`   ${timeframe}: ✅ ${coverage.count} candles, no gaps`);
            }
        }
    }

    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total gaps found: ${totalGaps}`);
    
    if (autoFix) {
        console.log(`Total candles inserted: ${totalFixed}`);
    }

    if (gapReport.length > 0 && !autoFix) {
        console.log('\n⚠️  Gap Details:');
        for (const report of gapReport) {
            console.log(`   ${report.symbol} ${report.timeframe}: ${report.gaps} gaps`);
            for (const gap of report.details.slice(0, 3)) {
                console.log(`      - ${gap.from.toISOString()} to ${gap.to.toISOString()} (${gap.missingCandles} candles)`);
            }
            if (report.details.length > 3) {
                console.log(`      ... and ${report.details.length - 3} more`);
            }
        }
        console.log('\n💡 Run with --fix to automatically backfill gaps');
    }

    console.log('='.repeat(60));

    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Error:', error);
    process.exit(1);
});