#!/usr/bin/env node
// =============================================================================
// scripts/quick-fix-incomplete.js - Fix Incomplete Candles NOW
// =============================================================================
//
// Run this to fix all candles where OHLC are identical (incomplete)
// Uses DELETE + INSERT from Dukascopy
//
// Usage:
//   node scripts/quick-fix-incomplete.js              # Fix last 24 hours
//   node scripts/quick-fix-incomplete.js --hours 48   # Fix last 48 hours
//   node scripts/quick-fix-incomplete.js --days 7     # Fix last 7 days
//   node scripts/quick-fix-incomplete.js EURUSD       # Fix specific symbol
// =============================================================================

require('dotenv').config();

const database = require('../database');
const { DukascopyBackfill } = require('../services/dukascopy-backfill');

const ALL_FOREX_SYMBOLS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    'EURGBP', 'EURJPY', 'GBPJPY', 'EURCHF', 'GBPCHF', 'AUDJPY',
    'EURAUD', 'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
    'NZDJPY', 'CADJPY', 'XAUUSD', 'XAGUSD'
];

async function findIncomplete(symbol, timeframe, from, to) {
    const [rows] = await database.pool.execute(`
        SELECT COUNT(*) as count
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp >= ? AND timestamp < ?
        AND open = high AND high = low AND low = close
    `, [symbol, timeframe, from, to]);
    
    return rows[0].count;
}

async function main() {
    const args = process.argv.slice(2);
    
    // Parse args
    let hours = 24;
    let symbols = [];
    
    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--hours' && args[i + 1]) {
            hours = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--days' && args[i + 1]) {
            hours = parseInt(args[i + 1]) * 24;
            i++;
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }
    
    if (symbols.length === 0) {
        symbols = ALL_FOREX_SYMBOLS;
    }
    
    const to = new Date();
    const from = new Date(to.getTime() - hours * 60 * 60 * 1000);
    
    console.log('='.repeat(60));
    console.log('🔧 Quick Fix Incomplete Candles');
    console.log('='.repeat(60));
    console.log(`📅 Period: Last ${hours} hours`);
    console.log(`📅 From: ${from.toISOString()}`);
    console.log(`📅 To: ${to.toISOString()}`);
    console.log(`💱 Symbols: ${symbols.length}`);
    console.log('='.repeat(60));
    
    await database.connect();
    
    const backfill = new DukascopyBackfill();
    let totalFixed = 0;
    
    for (const symbol of symbols) {
        // Check how many incomplete candles exist
        const incompleteCount = await findIncomplete(symbol, 'M1', from, to);
        
        if (incompleteCount === 0) {
            console.log(`✅ ${symbol}: No incomplete candles`);
            continue;
        }
        
        console.log(`\n⚠️ ${symbol}: ${incompleteCount} incomplete M1 candles`);
        console.log(`   Fixing with DELETE + INSERT from Dukascopy...`);
        
        try {
            const fixed = await backfill.fixIncompleteCandles(symbol, 'M1', from, to);
            totalFixed += fixed;
            console.log(`   ✅ Fixed ${fixed} candles`);
            
            // Also rebuild higher timeframes
            console.log(`   🔄 Rebuilding higher timeframes...`);
            await backfill.rebuildHigherTimeframes(symbol, from, to);
            
        } catch (error) {
            console.error(`   ❌ Error: ${error.message}`);
        }
        
        // Small delay between symbols
        await new Promise(r => setTimeout(r, 1000));
    }
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total candles fixed: ${totalFixed}`);
    console.log(`Dukascopy requests: ${backfill.getStats().requestsMade}`);
    console.log('='.repeat(60));
    
    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});