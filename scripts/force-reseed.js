#!/usr/bin/env node
// =============================================================================
// scripts/force-reseed.js - Nuclear Option: Delete & Re-insert Candles
// =============================================================================
//
// This script DELETES existing candles in a date range, then re-fetches
// fresh data from Dukascopy. Use when ON DUPLICATE KEY UPDATE isn't working.
//
// Usage:
//   node scripts/force-reseed.js EURUSD H4 --days 30
//   node scripts/force-reseed.js EURUSD M1 --from 2025-11-01 --to 2025-12-01
//   node scripts/force-reseed.js EURUSD --all-timeframes --days 7
// =============================================================================

require('dotenv').config();

const database = require('../database');
const dukascopy = require('../services/dukascopy-service');

const ALL_TIMEFRAMES = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'];

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseDate(str) {
    const d = new Date(str);
    if (isNaN(d.getTime())) {
        throw new Error(`Invalid date: ${str}`);
    }
    return d;
}

/**
 * Delete candles in a specific range
 */
async function deleteCandles(symbol, timeframe, fromDate, toDate) {
    const [result] = await database.pool.execute(`
        DELETE FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp >= ? AND timestamp <= ?
    `, [symbol, timeframe, fromDate, toDate]);

    return result.affectedRows;
}

/**
 * Count existing candles
 */
async function countCandles(symbol, timeframe, fromDate, toDate) {
    const [rows] = await database.pool.execute(`
        SELECT COUNT(*) as count
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = ?
        AND timestamp >= ? AND timestamp <= ?
    `, [symbol, timeframe, fromDate, toDate]);

    return rows[0].count;
}

/**
 * Fetch and insert fresh candles (simple INSERT, not UPSERT)
 */
async function insertFreshCandles(symbol, timeframe, fromDate, toDate) {
    // Fetch from Dukascopy
    const candles = await dukascopy.fetchCandles(symbol, timeframe, fromDate, toDate);
    
    if (candles.length === 0) {
        return 0;
    }

    // Insert in batches
    const batchSize = 500;
    let inserted = 0;

    for (let i = 0; i < candles.length; i += batchSize) {
        const batch = candles.slice(i, i + batchSize);
        
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
        const values = batch.flatMap(c => [
            c.symbol, c.timeframe, c.timestamp,
            c.open, c.high, c.low, c.close, c.volume, 0
        ]);

        try {
            const [result] = await database.pool.execute(`
                INSERT INTO pulse_market_data 
                (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                VALUES ${placeholders}
            `, values);

            inserted += result.affectedRows;
        } catch (error) {
            // If duplicate error, try individual inserts
            if (error.code === 'ER_DUP_ENTRY') {
                for (const c of batch) {
                    try {
                        await database.pool.execute(`
                            INSERT INTO pulse_market_data 
                            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        `, [c.symbol, c.timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume, 0]);
                        inserted++;
                    } catch (e) {
                        // Skip
                    }
                }
            } else {
                console.error('Insert error:', error.message);
            }
        }
    }

    return inserted;
}

/**
 * Force reseed a symbol/timeframe combination
 */
async function forceReseed(symbol, timeframe, fromDate, toDate) {
    console.log(`\n📊 ${symbol} ${timeframe}`);
    console.log(`   📅 Range: ${fromDate.toISOString().split('T')[0]} to ${toDate.toISOString().split('T')[0]}`);

    // Count existing
    const existingCount = await countCandles(symbol, timeframe, fromDate, toDate);
    console.log(`   📈 Existing candles: ${existingCount}`);

    // Delete existing
    if (existingCount > 0) {
        const deleted = await deleteCandles(symbol, timeframe, fromDate, toDate);
        console.log(`   🗑️  Deleted: ${deleted} candles`);
    }

    // Fetch and insert fresh data
    console.log(`   📥 Fetching fresh data from Dukascopy...`);
    
    try {
        const inserted = await insertFreshCandles(symbol, timeframe, fromDate, toDate);
        console.log(`   ✅ Inserted: ${inserted} fresh candles`);
        return { deleted: existingCount, inserted };
    } catch (error) {
        console.log(`   ❌ Error: ${error.message}`);
        return { deleted: existingCount, inserted: 0, error: error.message };
    }
}

/**
 * Force reseed with chunking for large date ranges
 */
async function forceReseedChunked(symbol, timeframe, fromDate, toDate) {
    const chunkDays = {
        'M1': 7, 'M5': 14, 'M15': 30, 'M30': 60,
        'H1': 90, 'H4': 180, 'D1': 365
    };

    const days = chunkDays[timeframe] || 30;
    const chunks = [];
    
    let currentStart = new Date(fromDate);
    while (currentStart < toDate) {
        const chunkEnd = new Date(currentStart);
        chunkEnd.setDate(chunkEnd.getDate() + days);
        
        chunks.push({
            from: new Date(currentStart),
            to: chunkEnd > toDate ? new Date(toDate) : chunkEnd
        });
        
        currentStart = new Date(chunkEnd);
    }

    console.log(`\n📊 ${symbol} ${timeframe} (${chunks.length} chunks)`);

    let totalDeleted = 0;
    let totalInserted = 0;

    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        process.stdout.write(`   📦 Chunk ${i + 1}/${chunks.length}: ${chunk.from.toISOString().split('T')[0]} to ${chunk.to.toISOString().split('T')[0]}...`);

        // Delete
        const deleted = await deleteCandles(symbol, timeframe, chunk.from, chunk.to);
        totalDeleted += deleted;

        // Insert
        try {
            const inserted = await insertFreshCandles(symbol, timeframe, chunk.from, chunk.to);
            totalInserted += inserted;
            console.log(` ✅ -${deleted} / +${inserted}`);
        } catch (error) {
            console.log(` ❌ ${error.message}`);
        }

        await sleep(500);
    }

    console.log(`   📊 Total: deleted ${totalDeleted}, inserted ${totalInserted}`);
    return { deleted: totalDeleted, inserted: totalInserted };
}

async function main() {
    const args = process.argv.slice(2);

    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        console.log(`
🔥 Force Reseed - Nuclear Option

This script DELETES existing candles and re-inserts fresh data from Dukascopy.
Use when normal update/upsert operations aren't working correctly.

Usage:
  node scripts/force-reseed.js <symbol> [timeframe] [options]

Options:
  --days <n>            Number of days back from today (default: 7)
  --from <date>         Start date (YYYY-MM-DD)
  --to <date>           End date (YYYY-MM-DD)
  --all-timeframes      Process all timeframes (M1, M5, M15, M30, H1, H4, D1)

Examples:
  node scripts/force-reseed.js EURUSD H4 --days 30
  node scripts/force-reseed.js EURUSD M1 --days 7
  node scripts/force-reseed.js EURUSD --all-timeframes --days 14
  node scripts/force-reseed.js XAUUSD D1 --from 2025-01-01 --to 2025-12-01

⚠️  WARNING: This DELETES data before re-inserting. Make sure you want to do this!
        `);
        process.exit(0);
    }

    // Parse arguments
    let symbol = null;
    let timeframes = [];
    let days = 7;
    let fromDate = null;
    let toDate = null;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--days' && args[i + 1]) {
            days = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--from' && args[i + 1]) {
            fromDate = parseDate(args[i + 1]);
            i++;
        } else if (args[i] === '--to' && args[i + 1]) {
            toDate = parseDate(args[i + 1]);
            i++;
        } else if (args[i] === '--all-timeframes') {
            timeframes = ALL_TIMEFRAMES;
        } else if (!args[i].startsWith('--')) {
            if (!symbol) {
                symbol = args[i].toUpperCase();
            } else if (ALL_TIMEFRAMES.includes(args[i].toUpperCase())) {
                timeframes.push(args[i].toUpperCase());
            }
        }
    }

    if (!symbol) {
        console.error('❌ Symbol is required');
        process.exit(1);
    }

    if (timeframes.length === 0) {
        console.error('❌ At least one timeframe is required (or use --all-timeframes)');
        process.exit(1);
    }

    // Calculate date range
    if (!toDate) {
        toDate = new Date();
    }
    if (!fromDate) {
        fromDate = new Date(toDate.getTime() - days * 24 * 60 * 60 * 1000);
    }

    await database.connect();

    console.log('='.repeat(60));
    console.log('🔥 FORCE RESEED - Nuclear Option');
    console.log('='.repeat(60));
    console.log(`💱 Symbol: ${symbol}`);
    console.log(`📊 Timeframes: ${timeframes.join(', ')}`);
    console.log(`📅 Range: ${fromDate.toISOString().split('T')[0]} to ${toDate.toISOString().split('T')[0]}`);
    console.log('='.repeat(60));
    console.log('⚠️  This will DELETE existing data and replace with fresh data!');
    console.log('='.repeat(60));

    let totalDeleted = 0;
    let totalInserted = 0;

    for (const timeframe of timeframes) {
        const result = await forceReseedChunked(symbol, timeframe, fromDate, toDate);
        totalDeleted += result.deleted;
        totalInserted += result.inserted;
        await sleep(1000);
    }

    console.log('\n' + '='.repeat(60));
    console.log('📊 SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total deleted: ${totalDeleted.toLocaleString()}`);
    console.log(`Total inserted: ${totalInserted.toLocaleString()}`);
    console.log('='.repeat(60));

    await database.disconnect();
}

main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});

module.exports = { forceReseed, forceReseedChunked, deleteCandles };