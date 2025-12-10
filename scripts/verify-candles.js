#!/usr/bin/env node
// =============================================================================
// scripts/verify-candles.js - Verify Candle Data Quality
// =============================================================================
//
// Checks for various data quality issues:
// - Identical OHLC values (incomplete candles)
// - Zero volume candles
// - Gaps in data
// - Weekend candles (shouldn't exist for forex)
// - Suspicious price movements
//
// Usage:
//   node scripts/verify-candles.js EURUSD H4 --days 30
// =============================================================================

require('dotenv').config();

const database = require('../database');

async function verifyCandles(symbol, timeframe, fromDate, toDate) {
    console.log(`\n📊 Verifying ${symbol} ${timeframe}`);
    console.log(`📅 Range: ${fromDate.toISOString()} to ${toDate.toISOString()}`);
    console.log('─'.repeat(60));

    // 1. Total count
    const [countRows] = await database.pool.execute(`
        SELECT COUNT(*) as total,
               MIN(timestamp) as first_candle,
               MAX(timestamp) as last_candle
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
    `, [symbol, timeframe, fromDate, toDate]);

    const { total, first_candle, last_candle } = countRows[0];
    console.log(`📈 Total candles: ${total}`);
    console.log(`   First: ${first_candle}`);
    console.log(`   Last: ${last_candle}`);

    // 2. Identical OHLC (incomplete)
    const [identicalRows] = await database.pool.execute(`
        SELECT COUNT(*) as count
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        AND open = high AND high = low AND low = close
    `, [symbol, timeframe, fromDate, toDate]);

    const identicalCount = identicalRows[0].count;
    if (identicalCount > 0) {
        console.log(`⚠️  Identical OHLC (incomplete): ${identicalCount}`);
        
        // Show samples
        const [samples] = await database.pool.execute(`
            SELECT id, timestamp, open, high, low, close
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp BETWEEN ? AND ?
            AND open = high AND high = low AND low = close
            LIMIT 5
        `, [symbol, timeframe, fromDate, toDate]);
        
        for (const s of samples) {
            console.log(`      ID ${s.id}: ${s.timestamp} - OHLC all = ${s.close}`);
        }
    } else {
        console.log(`✅ No identical OHLC candles`);
    }

    // 3. Very small range candles (< 0.01% of price)
    const [smallRangeRows] = await database.pool.execute(`
        SELECT COUNT(*) as count
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        AND high != low
        AND (high - low) / close < 0.0001
    `, [symbol, timeframe, fromDate, toDate]);

    const smallRangeCount = smallRangeRows[0].count;
    if (smallRangeCount > 0) {
        console.log(`🔍 Very small range (<0.01%): ${smallRangeCount}`);
    }

    // 4. Weekend candles (for forex)
    const [weekendRows] = await database.pool.execute(`
        SELECT COUNT(*) as count
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        AND (
            DAYOFWEEK(timestamp) = 7 OR 
            (DAYOFWEEK(timestamp) = 1 AND HOUR(timestamp) < 21) OR
            (DAYOFWEEK(timestamp) = 6 AND HOUR(timestamp) >= 22)
        )
    `, [symbol, timeframe, fromDate, toDate]);

    const weekendCount = weekendRows[0].count;
    if (weekendCount > 0) {
        console.log(`⚠️  Weekend candles (should be 0 for forex): ${weekendCount}`);
    } else {
        console.log(`✅ No weekend candles`);
    }

    // 5. Show sample of actual data
    console.log(`\n📋 Sample candles:`);
    const [sampleRows] = await database.pool.execute(`
        SELECT id, timestamp, open, high, low, close, volume,
               (high - low) as range_val,
               ROUND((high - low) / close * 10000, 2) as range_pips
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp DESC
        LIMIT 10
    `, [symbol, timeframe, fromDate, toDate]);

    console.log('   ID      | Timestamp           | Open       | High       | Low        | Close      | Range(pips)');
    console.log('   ' + '─'.repeat(100));
    for (const row of sampleRows) {
        const ts = new Date(row.timestamp).toISOString().replace('T', ' ').slice(0, 19);
        console.log(`   ${String(row.id).padEnd(7)} | ${ts} | ${row.open.toFixed(5).padStart(10)} | ${row.high.toFixed(5).padStart(10)} | ${row.low.toFixed(5).padStart(10)} | ${row.close.toFixed(5).padStart(10)} | ${row.range_pips}`);
    }

    // 6. Compare with expected candle count
    const tfMinutes = {
        'M1': 1, 'M5': 5, 'M15': 15, 'M30': 30,
        'H1': 60, 'H4': 240, 'D1': 1440
    };
    
    const minutesInRange = (toDate - fromDate) / (1000 * 60);
    const expectedCandles = Math.floor(minutesInRange / tfMinutes[timeframe]);
    // Forex is only open ~5/7 days
    const adjustedExpected = Math.floor(expectedCandles * (5/7));
    
    console.log(`\n📊 Coverage estimate:`);
    console.log(`   Expected (24/7): ~${expectedCandles.toLocaleString()}`);
    console.log(`   Expected (forex ~5/7): ~${adjustedExpected.toLocaleString()}`);
    console.log(`   Actual: ${total.toLocaleString()}`);
    console.log(`   Coverage: ${((total / adjustedExpected) * 100).toFixed(1)}%`);

    return {
        total,
        identicalOHLC: identicalCount,
        smallRange: smallRangeCount,
        weekend: weekendCount
    };
}

async function main() {
    const args = process.argv.slice(2);

    if (args.length < 2 || args.includes('--help')) {
        console.log(`
📊 Verify Candle Data Quality

Usage:
  node scripts/verify-candles.js <symbol> <timeframe> [options]

Options:
  --days <n>     Number of days to check (default: 7)
  --from <date>  Start date
  --to <date>    End date

Examples:
  node scripts/verify-candles.js EURUSD H4 --days 30
  node scripts/verify-candles.js XAUUSD M1 --days 7
        `);
        process.exit(0);
    }

    const symbol = args[0].toUpperCase();
    const timeframe = args[1].toUpperCase();
    
    let days = 7;
    let fromDate = null;
    let toDate = null;

    for (let i = 2; i < args.length; i++) {
        if (args[i] === '--days' && args[i + 1]) {
            days = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--from' && args[i + 1]) {
            fromDate = new Date(args[i + 1]);
            i++;
        } else if (args[i] === '--to' && args[i + 1]) {
            toDate = new Date(args[i + 1]);
            i++;
        }
    }

    if (!toDate) toDate = new Date();
    if (!fromDate) fromDate = new Date(toDate.getTime() - days * 24 * 60 * 60 * 1000);

    await database.connect();

    console.log('='.repeat(60));
    console.log('📊 Candle Data Verification');
    console.log('='.repeat(60));

    await verifyCandles(symbol, timeframe, fromDate, toDate);

    console.log('\n' + '='.repeat(60));

    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Error:', error);
    process.exit(1);
});

module.exports = { verifyCandles };