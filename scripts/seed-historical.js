#!/usr/bin/env node
// =============================================================================
// scripts/seed-historical.js - Seed Historical Data from Dukascopy
// =============================================================================

require('dotenv').config();

const database = require('../database');
const dukascopy = require('../services/dukascopy-service');

// Configuration
const CONFIG = {
    yearsToFetch: 5,
    timeframes: ['D1', 'H4', 'H1', 'M15','M30', 'M5', 'M1'],
    chunkDays: {
        'M1': 7,
        'M5': 30,
        'M15': 60,
        'M30': 90,
        'H1': 180,
        'H4': 365,
        'D1': 365 * 2
    },
    requestDelay: 1000,
    symbols: [
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
        'XAUUSD', 'XAGUSD',
        'EURGBP', 'EURJPY', 'GBPJPY', 'EURCHF', 'GBPCHF', 'AUDJPY',
        'EURAUD', 'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
        'NZDJPY', 'CADJPY'
    ]
};

let totalProgress = {
    symbol: '',
    timeframe: '',
    currentChunk: 0,
    totalChunks: 0,
    totalCandles: 0,
    startTime: null
};

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function formatDuration(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
}

async function seedSymbolTimeframe(symbol, timeframe, fromDate, toDate) {
    const chunkDays = CONFIG.chunkDays[timeframe] || 30;
    const chunks = [];
    
    let currentStart = new Date(fromDate);
    while (currentStart < toDate) {
        const chunkEnd = new Date(currentStart);
        chunkEnd.setDate(chunkEnd.getDate() + chunkDays);
        
        chunks.push({
            from: new Date(currentStart),
            to: chunkEnd > toDate ? new Date(toDate) : chunkEnd
        });
        
        currentStart = new Date(chunkEnd);
    }

    totalProgress.totalChunks = chunks.length;
    let totalInserted = 0;

    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        totalProgress.currentChunk = i + 1;

        process.stdout.write(`\r   📦 Chunk ${i + 1}/${chunks.length}: ${chunk.from.toISOString().split('T')[0]} to ${chunk.to.toISOString().split('T')[0]}...`);

        try {
            const inserted = await dukascopy.fetchAndSave(symbol, timeframe, chunk.from, chunk.to);
            totalInserted += inserted;
            totalProgress.totalCandles += inserted;
        } catch (error) {
            console.error(`\n   ❌ Error: ${error.message}`);
        }

        if (i < chunks.length - 1) {
            await sleep(CONFIG.requestDelay);
        }
    }

    console.log(`\n   ✅ Total inserted for ${symbol} ${timeframe}: ${totalInserted.toLocaleString()} candles`);
    return totalInserted;
}

async function seedAll(options = {}) {
    const { years, months } = options;
    
    // Calculate date range
    const endDate = new Date();
    const startDate = new Date();
    
    if (months) {
        startDate.setMonth(startDate.getMonth() - months);
    } else {
        startDate.setFullYear(startDate.getFullYear() - (years || CONFIG.yearsToFetch));
    }

    const rangeLabel = months ? `${months} month(s)` : `${years || CONFIG.yearsToFetch} year(s)`;

    console.log('='.repeat(60));
    console.log('🌱 PulseMarkets Historical Data Seeder');
    console.log('='.repeat(60));
    console.log(`📅 Fetching ${rangeLabel} of historical data`);
    console.log(`📊 Timeframes: ${CONFIG.timeframes.join(', ')}`);
    console.log(`💱 Symbols: ${CONFIG.symbols.length} pairs`);
    console.log(`📆 Range: ${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`);
    console.log('='.repeat(60));

    await database.connect();

    totalProgress.startTime = Date.now();
    dukascopy.resetStats();

    const totalCombinations = CONFIG.symbols.length * CONFIG.timeframes.length;
    let currentCombination = 0;

    for (const symbol of CONFIG.symbols) {
        console.log(`\n${'─'.repeat(50)}`);
        console.log(`💱 Processing ${symbol}`);
        console.log('─'.repeat(50));

        for (const timeframe of CONFIG.timeframes) {
            currentCombination++;
            totalProgress.symbol = symbol;
            totalProgress.timeframe = timeframe;

            const progress = ((currentCombination / totalCombinations) * 100).toFixed(1);
            console.log(`\n📈 [${progress}%] ${symbol} ${timeframe}`);

            const latestExisting = await dukascopy.getLatestTimestamp(symbol, timeframe);
            
            let fetchFrom = startDate;
            if (latestExisting) {
                fetchFrom = new Date(latestExisting);
                fetchFrom.setMinutes(fetchFrom.getMinutes() + 1);
                console.log(`   📌 Existing data found, starting from ${fetchFrom.toISOString().split('T')[0]}`);
            }

            if (fetchFrom >= endDate) {
                console.log(`   ⭐️ Already up to date, skipping`);
                continue;
            }

            await seedSymbolTimeframe(symbol, timeframe, fetchFrom, endDate);
            await sleep(CONFIG.requestDelay * 2);
        }
    }

    const duration = Date.now() - totalProgress.startTime;
    const stats = dukascopy.getStats();

    console.log('\n' + '='.repeat(60));
    console.log('🎉 SEEDING COMPLETE');
    console.log('='.repeat(60));
    console.log(`⏱️  Duration: ${formatDuration(duration)}`);
    console.log(`📊 Total candles fetched: ${stats.fetched.toLocaleString()}`);
    console.log(`💾 Total candles inserted: ${stats.inserted.toLocaleString()}`);
    console.log(`❌ Errors: ${stats.errors}`);
    console.log('='.repeat(60));

    await database.disconnect();
}

async function seedSymbols(symbols, options = {}) {
    const { timeframes = CONFIG.timeframes, years, months } = options;
    
    await database.connect();

    const endDate = new Date();
    const startDate = new Date();
    
    if (months) {
        startDate.setMonth(startDate.getMonth() - months);
    } else {
        startDate.setFullYear(startDate.getFullYear() - (years || CONFIG.yearsToFetch));
    }

    const rangeLabel = months ? `${months} month(s)` : `${years || CONFIG.yearsToFetch} year(s)`;
    console.log(`\n🌱 Seeding ${symbols.join(', ')} for ${rangeLabel}`);
    console.log(`📆 Range: ${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`);

    for (const symbol of symbols) {
        for (const timeframe of timeframes) {
            console.log(`\n📈 ${symbol} ${timeframe}`);
            await seedSymbolTimeframe(symbol, timeframe, startDate, endDate);
            await sleep(CONFIG.requestDelay);
        }
    }

    await database.disconnect();
}

async function main() {
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
        await seedAll();
        return;
    }
    
    if (args[0] === '--help' || args[0] === '-h') {
        console.log(`
🌱 PulseMarkets Historical Data Seeder

Usage:
  node scripts/seed-historical.js              # Seed all symbols (5 years)
  node scripts/seed-historical.js EURUSD       # Seed specific symbol
  node scripts/seed-historical.js EURUSD GBPUSD # Seed multiple symbols

Options:
  --years <n>       Number of years to fetch (default: 5)
  --months <n>      Number of months to fetch (overrides --years)
  --timeframe <tf>  Specific timeframe (M1, M5, M15, M30, H1, H4, D1)
  --help, -h        Show this help

Examples:
  node scripts/seed-historical.js --months 1           # Last 1 month, all symbols
  node scripts/seed-historical.js --months 3           # Last 3 months, all symbols
  node scripts/seed-historical.js EURUSD --months 1    # Last 1 month, EURUSD only
  node scripts/seed-historical.js EURUSD --years 3     # Last 3 years, EURUSD only
  node scripts/seed-historical.js XAUUSD --timeframe D1 --months 6
  
Note: Duplicates are automatically skipped (INSERT IGNORE on unique key).
        `);
        return;
    }

    // Parse arguments
    let symbols = [];
    let years = null;
    let months = null;
    let timeframes = CONFIG.timeframes;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--years' && args[i + 1]) {
            years = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--months' && args[i + 1]) {
            months = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--timeframe' && args[i + 1]) {
            timeframes = [args[i + 1].toUpperCase()];
            i++;
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }

    const options = { years, months, timeframes };

    if (symbols.length > 0) {
        await seedSymbols(symbols, options);
    } else {
        await seedAll(options);
    }
}

main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});

module.exports = { seedAll, seedSymbols, CONFIG };