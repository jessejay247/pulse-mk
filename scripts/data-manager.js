#!/usr/bin/env node
// =============================================================================
// scripts/data-manager.js - Automated Data Management with Cron Jobs
// =============================================================================
// 
// This script runs continuously and handles:
// ✔ Every minute: Fetch latest candles from Dukascopy
// ✔ Every night (2 AM): Backfill any missing data gaps
// ✔ Every Sunday (3 AM): Aggregate/compress old M1 data into larger timeframes
// ✔ Every month: Cleanup very old M1 data (keep only aggregated)
//
// Run with: node scripts/data-manager.js
// Or with PM2: pm2 start scripts/data-manager.js --name "data-manager"
// =============================================================================

require('dotenv').config();

const cron = require('node-cron');
const database = require('../database');
const dukascopy = require('../services/dukascopy-service');

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
    // Symbols to maintain
    symbols: {
        // Majors - update every minute
        majors: ['EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD'],
        
        // Metals - update every minute
        metals: ['XAUUSD', 'XAGUSD'],
        
        // Minors - update every 5 minutes
        minors: ['EURGBP', 'EURJPY', 'GBPJPY', 'EURCHF', 'GBPCHF', 'AUDJPY',
                 'EURAUD', 'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
                 'NZDJPY', 'CADJPY']
    },
    
    // Timeframes to maintain
    timeframes: ['M1', 'M5', 'M15','M30', 'H1', 'H4', 'D1'],
    
    // Data retention (days)
    retention: {
        M1: 30,      // Keep M1 data for 30 days
        M5: 90,      // Keep M5 data for 90 days
        M15: 180,    // Keep M15 data for 6 months
        M30: 365,    // Keep M30 data for 1 year
        H1: 365 * 2, // Keep H1 data for 2 years
        H4: 365 * 5, // Keep H4 data for 5 years
        D1: 365 * 10 // Keep D1 data for 10 years
    },
    
    // Delay between API calls (ms)
    requestDelay: 500
};

// State tracking
const state = {
    isRunning: false,
    lastMinuteUpdate: null,
    lastNightlyBackfill: null,
    lastWeeklyAggregate: null,
    errors: []
};

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function log(message, level = 'info') {
    const timestamp = new Date().toISOString();
    const prefix = {
        info: '📊',
        success: '✅',
        warning: '⚠️',
        error: '❌',
        cron: '⏰'
    }[level] || '📊';
    
    console.log(`[${timestamp}] ${prefix} ${message}`);
}

function getAllSymbols() {
    return [
        ...CONFIG.symbols.majors,
        ...CONFIG.symbols.metals,
        ...CONFIG.symbols.minors
    ];
}

// =============================================================================
// TASK 1: MINUTE UPDATE - Fetch Latest Data
// =============================================================================

async function minuteUpdate() {
    if (state.isRunning) {
        log('Previous task still running, skipping minute update', 'warning');
        return;
    }

    state.isRunning = true;
    const startTime = Date.now();

    try {
        log('Starting minute update...', 'cron');
        
        const now = new Date();
        const fiveMinutesAgo = new Date(now.getTime() - 5 * 60 * 1000);
        
        // Update majors and metals with M1
        const prioritySymbols = [...CONFIG.symbols.majors, ...CONFIG.symbols.metals];
        
        for (const symbol of prioritySymbols) {
            try {
                await dukascopy.fetchAndSave(symbol, 'M1', fiveMinutesAgo, now);
                await sleep(CONFIG.requestDelay);
            } catch (error) {
                log(`Error updating ${symbol}: ${error.message}`, 'error');
            }
        }

        state.lastMinuteUpdate = new Date();
        const duration = Date.now() - startTime;
        log(`Minute update completed in ${duration}ms`, 'success');
        
    } catch (error) {
        log(`Minute update failed: ${error.message}`, 'error');
        state.errors.push({ time: new Date(), task: 'minuteUpdate', error: error.message });
    } finally {
        state.isRunning = false;
    }
}

// Every 5 minutes: Update minor pairs
async function fiveMinuteUpdate() {
    if (state.isRunning) return;

    state.isRunning = true;

    try {
        log('Starting 5-minute update for minor pairs...', 'cron');
        
        const now = new Date();
        const tenMinutesAgo = new Date(now.getTime() - 10 * 60 * 1000);
        
        for (const symbol of CONFIG.symbols.minors) {
            try {
                await dukascopy.fetchAndSave(symbol, 'M1', tenMinutesAgo, now);
                await dukascopy.fetchAndSave(symbol, 'M5', tenMinutesAgo, now);
                await sleep(CONFIG.requestDelay);
            } catch (error) {
                log(`Error updating ${symbol}: ${error.message}`, 'error');
            }
        }

        log('5-minute update completed', 'success');
        
    } catch (error) {
        log(`5-minute update failed: ${error.message}`, 'error');
    } finally {
        state.isRunning = false;
    }
}

// =============================================================================
// TASK 2: NIGHTLY BACKFILL - Fill Data Gaps
// =============================================================================

async function nightlyBackfill() {
    log('Starting nightly backfill...', 'cron');
    
    const symbols = getAllSymbols();
    const timeframes = ['M1', 'M5', 'M15','M30', 'H1'];
    
    // Look back 7 days for gaps
    const now = new Date();
    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    
    let totalGapsFound = 0;
    let totalGapsFilled = 0;

    for (const symbol of symbols) {
        for (const timeframe of timeframes) {
            try {
                // Detect gaps
                const gaps = await dukascopy.detectGaps(symbol, timeframe, weekAgo, now);
                
                if (gaps.length > 0) {
                    log(`Found ${gaps.length} gaps in ${symbol} ${timeframe}`, 'warning');
                    totalGapsFound += gaps.length;
                    
                    // Fill each gap
                    for (const gap of gaps) {
                        try {
                            const inserted = await dukascopy.fetchAndSave(
                                symbol,
                                timeframe,
                                gap.from,
                                gap.to
                            );
                            if (inserted > 0) {
                                totalGapsFilled++;
                                log(`Filled gap: ${symbol} ${timeframe} ${gap.from.toISOString()} - ${inserted} candles`, 'success');
                            }
                        } catch (error) {
                            log(`Failed to fill gap: ${error.message}`, 'error');
                        }
                        await sleep(CONFIG.requestDelay);
                    }
                }
            } catch (error) {
                log(`Error checking gaps for ${symbol} ${timeframe}: ${error.message}`, 'error');
            }
        }
        
        await sleep(CONFIG.requestDelay);
    }

    state.lastNightlyBackfill = new Date();
    log(`Nightly backfill completed: ${totalGapsFound} gaps found, ${totalGapsFilled} filled`, 'success');
}

// =============================================================================
// TASK 3: WEEKLY AGGREGATE - Build Higher Timeframes from M1
// =============================================================================

async function weeklyAggregate() {
    log('Starting weekly aggregation...', 'cron');
    
    const symbols = getAllSymbols();
    
    // Aggregate last 7 days of data
    const now = new Date();
    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    
    for (const symbol of symbols) {
        try {
            // Aggregate M1 -> M5
            await aggregateTimeframe(symbol, 'M1', 'M5', 5, weekAgo, now);
            
            // Aggregate M1 -> M15
            await aggregateTimeframe(symbol, 'M1', 'M15', 15, weekAgo, now);
            // Aggregate M1 -> M30
            await aggregateTimeframe(symbol, 'M1', 'M30', 30, weekAgo, now);
            
            // Aggregate M1 -> H1
            await aggregateTimeframe(symbol, 'M1', 'H1', 60, weekAgo, now);
            
            // Aggregate H1 -> H4
            await aggregateTimeframe(symbol, 'H1', 'H4', 4, weekAgo, now);
            
            // Aggregate H1 -> D1
            await aggregateTimeframe(symbol, 'H1', 'D1', 24, weekAgo, now);
            
            log(`Aggregated ${symbol}`, 'success');
            
        } catch (error) {
            log(`Error aggregating ${symbol}: ${error.message}`, 'error');
        }
        
        await sleep(100);
    }

    state.lastWeeklyAggregate = new Date();
    log('Weekly aggregation completed', 'success');
}

/**
 * Aggregate data from source timeframe to target timeframe
 */
async function aggregateTimeframe(symbol, sourceTimeframe, targetTimeframe, multiplier, from, to) {
    try {
        // Get source data
        const [rows] = await database.pool.execute(`
            SELECT timestamp, open, high, low, close, volume
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp ASC
        `, [symbol, sourceTimeframe, from, to]);

        if (rows.length === 0) return;

        // Group into target timeframe periods
        const aggregated = new Map();
        
        for (const row of rows) {
            const timestamp = new Date(row.timestamp);
            const periodStart = getTimeframePeriodStart(timestamp, targetTimeframe);
            const key = periodStart.getTime();
            
            if (!aggregated.has(key)) {
                aggregated.set(key, {
                    timestamp: periodStart,
                    open: parseFloat(row.open),
                    high: parseFloat(row.high),
                    low: parseFloat(row.low),
                    close: parseFloat(row.close),
                    volume: parseFloat(row.volume || 0)
                });
            } else {
                const candle = aggregated.get(key);
                candle.high = Math.max(candle.high, parseFloat(row.high));
                candle.low = Math.min(candle.low, parseFloat(row.low));
                candle.close = parseFloat(row.close);
                candle.volume += parseFloat(row.volume || 0);
            }
        }

        // Insert aggregated data
        const candles = Array.from(aggregated.values()).map(c => ({
            symbol,
            timeframe: targetTimeframe,
            ...c
        }));

        if (candles.length > 0) {
            await dukascopy.saveCandles(candles);
        }

    } catch (error) {
        log(`Aggregation error ${symbol} ${sourceTimeframe}->${targetTimeframe}: ${error.message}`, 'error');
    }
}

/**
 * Get the start of a timeframe period
 */
function getTimeframePeriodStart(date, timeframe) {
    const d = new Date(date);
    
    switch (timeframe) {
        case 'M5':
            d.setMinutes(Math.floor(d.getMinutes() / 5) * 5, 0, 0);
            break;
        case 'M15':
            d.setMinutes(Math.floor(d.getMinutes() / 15) * 15, 0, 0);
            break;
        case 'M30':
            d.setMinutes(Math.floor(d.getMinutes() / 30) * 30, 0, 0);
            break;
        case 'H1':
            d.setMinutes(0, 0, 0);
            break;
        case 'H4':
            d.setHours(Math.floor(d.getHours() / 4) * 4, 0, 0, 0);
            break;
        case 'D1':
            d.setHours(0, 0, 0, 0);
            break;
    }
    
    return d;
}

// =============================================================================
// TASK 4: MONTHLY CLEANUP - Remove Old Data
// =============================================================================

async function monthlyCleanup() {
    log('Starting monthly cleanup...', 'cron');
    
    const now = new Date();
    let totalDeleted = 0;

    for (const [timeframe, retentionDays] of Object.entries(CONFIG.retention)) {
        const cutoffDate = new Date(now.getTime() - retentionDays * 24 * 60 * 60 * 1000);
        
        try {
            const [result] = await database.pool.execute(`
                DELETE FROM pulse_market_data
                WHERE timeframe = ? AND timestamp < ?
            `, [timeframe, cutoffDate]);
            
            if (result.affectedRows > 0) {
                log(`Deleted ${result.affectedRows} old ${timeframe} candles (before ${cutoffDate.toISOString().split('T')[0]})`, 'info');
                totalDeleted += result.affectedRows;
            }
        } catch (error) {
            log(`Error cleaning ${timeframe}: ${error.message}`, 'error');
        }
    }

    log(`Monthly cleanup completed: ${totalDeleted} candles deleted`, 'success');
}

// =============================================================================
// STARTUP & CRON SCHEDULING
// =============================================================================

async function startup() {
    console.log('='.repeat(60));
    console.log('🚀 PulseMarkets Data Manager');
    console.log('='.repeat(60));
    console.log(`📅 Started at: ${new Date().toISOString()}`);
    console.log(`💱 Managing ${getAllSymbols().length} symbols`);
    console.log(`📊 Timeframes: ${CONFIG.timeframes.join(', ')}`);
    console.log('='.repeat(60));
    console.log('');
    console.log('⏰ Scheduled Tasks:');
    console.log('   • Every minute: Update major pairs & metals (M1)');
    console.log('   • Every 5 mins: Update minor pairs (M1, M5)');
    console.log('   • Every night (2 AM): Backfill data gaps');
    console.log('   • Every Sunday (3 AM): Aggregate timeframes');
    console.log('   • Every month (1st, 4 AM): Cleanup old data');
    console.log('');
    console.log('='.repeat(60));
    
    // Connect to database
    await database.connect();
    
    // Schedule cron jobs
    
    // Every minute - update majors and metals
    cron.schedule('* * * * *', async () => {
        await minuteUpdate();
    });
    
    // Every 5 minutes - update minor pairs
    cron.schedule('*/5 * * * *', async () => {
        await fiveMinuteUpdate();
    });
    
    // Every night at 2 AM - backfill gaps
    cron.schedule('0 2 * * *', async () => {
        await nightlyBackfill();
    });
    
    // Every Sunday at 3 AM - aggregate timeframes
    cron.schedule('0 3 * * 0', async () => {
        await weeklyAggregate();
    });
    
    // First of every month at 4 AM - cleanup old data
    cron.schedule('0 4 1 * *', async () => {
        await monthlyCleanup();
    });
    
    log('Data Manager started and cron jobs scheduled', 'success');
    
    // Run initial update
    log('Running initial data update...', 'info');
    await minuteUpdate();
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
    log('Received SIGTERM, shutting down...', 'warning');
    await database.disconnect();
    process.exit(0);
});

process.on('SIGINT', async () => {
    log('Received SIGINT, shutting down...', 'warning');
    await database.disconnect();
    process.exit(0);
});

// Start
startup().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});

module.exports = { minuteUpdate, nightlyBackfill, weeklyAggregate, monthlyCleanup, CONFIG };