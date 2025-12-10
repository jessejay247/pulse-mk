// =============================================================================
// scripts/manual-operations.js - Manual Data Operations CLI
// =============================================================================
//
// Usage:
//   node scripts/manual-operations.js rebuild EURUSD M1 --from 2025-12-01
//   node scripts/manual-operations.js backfill EURUSD --days 7
//   node scripts/manual-operations.js verify EURUSD H4 --days 30
//   node scripts/manual-operations.js gaps --primary
//   node scripts/manual-operations.js health
//   node scripts/manual-operations.js fix-incomplete EURUSD
// =============================================================================

require('dotenv').config();

const database = require('../database');
const { CandleBuilder } = require('../services/candle-builder');
const { GapDetector } = require('../services/gap-detector');
const { DukascopyBackfill } = require('../services/dukascopy-backfill');
const { HealthMonitor } = require('../services/health-monitor');

const PRIMARY_PAIRS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF',
    'AUDUSD', 'USDCAD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
];

const ALL_TIMEFRAMES = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'];

// =============================================================================
// COMMAND: rebuild
// =============================================================================

async function cmdRebuild(args) {
    const symbol = args[0]?.toUpperCase();
    const timeframe = args[1]?.toUpperCase() || 'M1';
    const from = parseDate(getArg(args, '--from')) || daysAgo(7);
    const to = parseDate(getArg(args, '--to')) || new Date();
    
    if (!symbol) {
        console.log('Usage: rebuild <symbol> [timeframe] --from <date> --to <date>');
        return;
    }
    
    console.log(`\n🔄 Rebuilding ${symbol} ${timeframe}`);
    console.log(`   From: ${from.toISOString()}`);
    console.log(`   To: ${to.toISOString()}\n`);
    
    const builder = new CandleBuilder();
    
    if (timeframe === 'M1') {
        // Rebuild M1 from ticks
        const candles = await builder.buildM1Range(symbol, from, to);
        console.log(`✅ Rebuilt ${candles.length} M1 candles`);
    } else {
        // Rebuild higher timeframe from M1
        let current = builder.getPeriodStart(from, timeframe);
        let count = 0;
        
        while (current <= to) {
            await builder.rebuildCandle(symbol, timeframe, current);
            count++;
            current = new Date(current.getTime() + builder.timeframeConfig[timeframe].minutes * 60000);
        }
        
        console.log(`✅ Rebuilt ${count} ${timeframe} candles`);
    }
}

// =============================================================================
// COMMAND: backfill
// =============================================================================

async function cmdBackfill(args) {
    const symbol = args[0]?.toUpperCase();
    const days = parseInt(getArg(args, '--days')) || 7;
    const timeframe = getArg(args, '--timeframe')?.toUpperCase() || 'M1';
    const tickLevel = hasArg(args, '--ticks');
    
    if (!symbol) {
        console.log('Usage: backfill <symbol> --days <n> [--timeframe M1] [--ticks]');
        return;
    }
    
    const from = daysAgo(days);
    const to = new Date();
    
    console.log(`\n📥 Backfilling ${symbol} ${timeframe}`);
    console.log(`   From: ${from.toISOString().split('T')[0]}`);
    console.log(`   To: ${to.toISOString().split('T')[0]}`);
    console.log(`   Tick-level: ${tickLevel}\n`);
    
    const backfill = new DukascopyBackfill();
    
    if (tickLevel && timeframe === 'M1') {
        // Tick-level backfill for M1
        const saved = await backfill.fetchTicksAndRebuildM1(symbol, from, to);
        console.log(`✅ Rebuilt ${saved} M1 candles from ticks`);
    } else {
        const saved = await backfill.fetchAndSave(symbol, timeframe, from, to);
        console.log(`✅ Backfilled ${saved} candles`);
    }
}

// =============================================================================
// COMMAND: verify
// =============================================================================

async function cmdVerify(args) {
    const symbol = args[0]?.toUpperCase();
    const timeframe = args[1]?.toUpperCase() || 'M1';
    const days = parseInt(getArg(args, '--days')) || 7;
    
    if (!symbol) {
        console.log('Usage: verify <symbol> [timeframe] --days <n>');
        return;
    }
    
    console.log(`\n🔍 Verifying ${symbol} ${timeframe} (last ${days} days)\n`);
    
    const detector = new GapDetector();
    const result = await detector.fullIntegrityCheck(symbol, timeframe, days);
    
    console.log('='.repeat(50));
    console.log(`Coverage: ${(result.coverage * 100).toFixed(1)}%`);
    console.log(`Gaps found: ${result.gaps.length}`);
    console.log(`Incomplete candles: ${result.incomplete.length}`);
    console.log(`Status: ${result.isHealthy ? '✅ Healthy' : '⚠️ Issues detected'}`);
    console.log('='.repeat(50));
    
    if (result.gaps.length > 0) {
        console.log('\nGaps:');
        for (const gap of result.gaps.slice(0, 10)) {
            console.log(`   ${gap.from.toISOString()} → ${gap.to.toISOString()} (${gap.missingCandles || '?'} candles)`);
        }
        if (result.gaps.length > 10) {
            console.log(`   ... and ${result.gaps.length - 10} more`);
        }
    }
    
    if (result.incomplete.length > 0) {
        console.log('\nIncomplete candles:');
        for (const c of result.incomplete.slice(0, 10)) {
            console.log(`   ${new Date(c.timestamp).toISOString()} - OHLC: ${c.close}`);
        }
        if (result.incomplete.length > 10) {
            console.log(`   ... and ${result.incomplete.length - 10} more`);
        }
    }
}

// =============================================================================
// COMMAND: gaps
// =============================================================================

async function cmdGaps(args) {
    const primaryOnly = hasArg(args, '--primary');
    const days = parseInt(getArg(args, '--days')) || 1;
    const autoFix = hasArg(args, '--fix');
    
    const symbols = primaryOnly ? PRIMARY_PAIRS : [...PRIMARY_PAIRS];
    
    console.log(`\n🔍 Scanning for gaps (last ${days} day(s))`);
    console.log(`   Symbols: ${symbols.length}`);
    console.log(`   Auto-fix: ${autoFix}\n`);
    
    const detector = new GapDetector();
    const backfill = new DukascopyBackfill();
    const from = daysAgo(days);
    const to = new Date();
    
    let totalGaps = 0;
    let totalFixed = 0;
    
    for (const symbol of symbols) {
        const gaps = await detector.detectGapsInRange(symbol, 'M1', from, to);
        
        if (gaps.length > 0) {
            console.log(`⚠️ ${symbol}: ${gaps.length} gaps`);
            totalGaps += gaps.length;
            
            if (autoFix) {
                for (const gap of gaps) {
                    const saved = await backfill.fetchAndSave(symbol, 'M1', gap.from, gap.to);
                    totalFixed += saved;
                    console.log(`   ✅ Fixed: ${gap.from.toISOString()} - ${saved} candles`);
                }
            }
        } else {
            console.log(`✅ ${symbol}: No gaps`);
        }
    }
    
    console.log(`\n📊 Total: ${totalGaps} gaps found`);
    if (autoFix) {
        console.log(`📊 Fixed: ${totalFixed} candles inserted`);
    } else if (totalGaps > 0) {
        console.log('💡 Run with --fix to auto-fill gaps');
    }
}

// =============================================================================
// COMMAND: health
// =============================================================================

async function cmdHealth(args) {
    console.log('\n🏥 Running health check...\n');
    
    const monitor = new HealthMonitor();
    const health = await monitor.check(PRIMARY_PAIRS);
    
    console.log('='.repeat(50));
    console.log(`Overall: ${health.overall === 'healthy' ? '✅ Healthy' : '⚠️ Degraded'}`);
    console.log(`Issues: ${health.issues.length}`);
    console.log('='.repeat(50));
    
    if (health.issues.length > 0) {
        console.log('\nIssues:');
        for (const issue of health.issues) {
            console.log(`   ❌ ${issue}`);
        }
    }
    
    console.log('\nData Freshness:');
    for (const [symbol, data] of Object.entries(health.metrics.freshness || {})) {
        const age = data.age ? `${Math.round(data.age / 60000)}m old` : 'no data';
        const icon = data.status === 'fresh' ? '✅' : '⚠️';
        console.log(`   ${icon} ${symbol}: ${age}`);
    }
    
    console.log('\nBackfill Queue:');
    const q = health.metrics.queueStatus || {};
    console.log(`   Pending: ${q.pending || 0}`);
    console.log(`   Processing: ${q.processing || 0}`);
    console.log(`   Failed: ${q.failed || 0}`);
}

// =============================================================================
// COMMAND: fix-incomplete
// =============================================================================

async function cmdFixIncomplete(args) {
    const symbol = args[0]?.toUpperCase();
    const days = parseInt(getArg(args, '--days')) || 7;
    
    if (!symbol) {
        console.log('Usage: fix-incomplete <symbol> --days <n>');
        return;
    }
    
    console.log(`\n🔧 Fixing incomplete candles for ${symbol} (last ${days} days)\n`);
    
    const detector = new GapDetector();
    const backfill = new DukascopyBackfill();
    const builder = new CandleBuilder();
    
    const from = daysAgo(days);
    const to = new Date();
    
    // Find incomplete candles
    const incomplete = await detector.findIncompleteCandles(symbol, 'M1', from, to);
    
    if (incomplete.length === 0) {
        console.log('✅ No incomplete candles found');
        return;
    }
    
    console.log(`Found ${incomplete.length} incomplete candles`);
    
    // Group into ranges
    const ranges = groupIntoRanges(incomplete.map(c => new Date(c.timestamp)));
    
    console.log(`Grouped into ${ranges.length} backfill ranges\n`);
    
    let totalFixed = 0;
    
    for (const range of ranges) {
        console.log(`📥 Backfilling ${range.from.toISOString()} → ${range.to.toISOString()}`);
        const saved = await backfill.fetchAndSave(symbol, 'M1', range.from, range.to);
        totalFixed += saved;
        console.log(`   ✅ ${saved} candles`);
    }
    
    console.log(`\n📊 Total fixed: ${totalFixed} candles`);
    
    // Rebuild higher timeframes
    console.log('\n🔄 Rebuilding higher timeframes...');
    await backfill.rebuildHigherTimeframes(symbol, from, to);
    console.log('✅ Higher timeframes rebuilt');
}

// =============================================================================
// UTILITIES
// =============================================================================

function getArg(args, flag) {
    const idx = args.indexOf(flag);
    return idx !== -1 && args[idx + 1] ? args[idx + 1] : null;
}

function hasArg(args, flag) {
    return args.includes(flag);
}

function parseDate(str) {
    if (!str) return null;
    const d = new Date(str);
    return isNaN(d.getTime()) ? null : d;
}

function daysAgo(n) {
    const d = new Date();
    d.setDate(d.getDate() - n);
    d.setUTCHours(0, 0, 0, 0);
    return d;
}

function groupIntoRanges(timestamps, gapThresholdMs = 5 * 60 * 1000) {
    if (timestamps.length === 0) return [];
    
    const sorted = timestamps.map(t => t.getTime()).sort((a, b) => a - b);
    const ranges = [];
    let rangeStart = sorted[0];
    let rangeEnd = sorted[0];
    
    for (let i = 1; i < sorted.length; i++) {
        if (sorted[i] - rangeEnd > gapThresholdMs) {
            ranges.push({ from: new Date(rangeStart - 60000), to: new Date(rangeEnd + 60000) });
            rangeStart = sorted[i];
        }
        rangeEnd = sorted[i];
    }
    
    ranges.push({ from: new Date(rangeStart - 60000), to: new Date(rangeEnd + 60000) });
    return ranges;
}

// =============================================================================
// MAIN
// =============================================================================

async function main() {
    const args = process.argv.slice(2);
    const command = args[0];
    const cmdArgs = args.slice(1);
    
    if (!command || command === '--help' || command === '-h') {
        console.log(`
📊 PulseMarkets Manual Operations CLI

Commands:
  rebuild <symbol> [timeframe]    Rebuild candles from ticks/lower TF
  backfill <symbol>               Fetch missing data from Dukascopy
  verify <symbol> [timeframe]     Check data integrity
  gaps                            Scan all symbols for gaps
  health                          Run health check
  fix-incomplete <symbol>         Fix candles with identical OHLC

Options:
  --days <n>        Number of days to process
  --from <date>     Start date (YYYY-MM-DD)
  --to <date>       End date (YYYY-MM-DD)
  --timeframe <tf>  Specific timeframe
  --primary         Primary pairs only
  --fix             Auto-fix issues
  --ticks           Use tick-level data

Examples:
  node scripts/manual-operations.js backfill EURUSD --days 7
  node scripts/manual-operations.js gaps --primary --fix
  node scripts/manual-operations.js verify XAUUSD H1 --days 30
  node scripts/manual-operations.js fix-incomplete GBPUSD --days 14
        `);
        return;
    }
    
    await database.connect();
    
    try {
        switch (command) {
            case 'rebuild':
                await cmdRebuild(cmdArgs);
                break;
            case 'backfill':
                await cmdBackfill(cmdArgs);
                break;
            case 'verify':
                await cmdVerify(cmdArgs);
                break;
            case 'gaps':
                await cmdGaps(cmdArgs);
                break;
            case 'health':
                await cmdHealth(cmdArgs);
                break;
            case 'fix-incomplete':
                await cmdFixIncomplete(cmdArgs);
                break;
            default:
                console.error(`Unknown command: ${command}`);
        }
    } catch (error) {
        console.error('❌ Error:', error.message);
    }
    
    await database.disconnect();
}

main();