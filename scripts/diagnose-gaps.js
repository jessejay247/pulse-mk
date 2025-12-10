#!/usr/bin/env node
// =============================================================================
// scripts/diagnose-gaps.js - Deep diagnosis of gap issues
// =============================================================================
// Usage: node scripts/diagnose-gaps.js EURUSD
// =============================================================================

require('dotenv').config();
const { getHistoricalRates } = require('dukascopy-node');
const database = require('../database');

const DUKASCOPY_INSTRUMENTS = {
    'EURUSD': 'eurusd', 'GBPUSD': 'gbpusd', 'USDJPY': 'usdjpy',
    'XAUUSD': 'xauusd', 'GBPJPY': 'gbpjpy',
};

async function diagnose(symbol) {
    const now = new Date();
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    
    console.log('═'.repeat(60));
    console.log(`🔍 DIAGNOSING ${symbol}`);
    console.log('═'.repeat(60));
    console.log(`Current time (UTC): ${now.toISOString()}`);
    console.log(`Current time (Local): ${now.toLocaleString()}`);
    console.log(`Checking: ${oneHourAgo.toISOString()} → ${now.toISOString()}`);
    console.log('');

    // ===========================================
    // 1. What's in the DATABASE?
    // ===========================================
    console.log('📊 DATABASE CANDLES:');
    
    const [dbCandles] = await database.pool.execute(`
        SELECT timestamp, open, close
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = 'M1'
        AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    `, [symbol, oneHourAgo, now]);
    
    console.log(`   Found: ${dbCandles.length} candles in last hour`);
    
    if (dbCandles.length > 0) {
        console.log(`   First: ${new Date(dbCandles[0].timestamp).toISOString()}`);
        console.log(`   Last:  ${new Date(dbCandles[dbCandles.length - 1].timestamp).toISOString()}`);
        
        // Find gaps
        const gaps = [];
        for (let i = 1; i < dbCandles.length; i++) {
            const prev = new Date(dbCandles[i - 1].timestamp).getTime();
            const curr = new Date(dbCandles[i].timestamp).getTime();
            const diff = (curr - prev) / 60000;
            
            if (diff > 1.5) {
                gaps.push({
                    from: new Date(prev + 60000),
                    to: new Date(curr),
                    minutes: Math.floor(diff) - 1
                });
            }
        }
        
        if (gaps.length > 0) {
            console.log(`\n   ⚠️  GAPS IN DATABASE:`);
            for (const g of gaps) {
                const ageMin = Math.floor((now.getTime() - g.to.getTime()) / 60000);
                console.log(`      ${g.from.toISOString().slice(11,16)} → ${g.to.toISOString().slice(11,16)} (${g.minutes} min missing, ended ${ageMin}m ago)`);
            }
        } else {
            console.log(`   ✅ No gaps in database!`);
        }
    }
    
    console.log('');
    
    // ===========================================
    // 2. What does DUKASCOPY have?
    // ===========================================
    console.log('🌐 DUKASCOPY DATA:');
    
    const instrument = DUKASCOPY_INSTRUMENTS[symbol];
    if (!instrument) {
        console.log(`   ❌ Symbol not supported by Dukascopy`);
        return;
    }
    
    try {
        // Try fetching last hour from Dukascopy
        const dukasData = await getHistoricalRates({
            instrument,
            dates: { from: oneHourAgo, to: now },
            timeframe: 'm1',
            format: 'json',
            priceType: 'bid',
            volumes: true,
        });
        
        console.log(`   Found: ${dukasData.length} candles from Dukascopy`);
        
        if (dukasData.length > 0) {
            const first = new Date(dukasData[0].timestamp);
            const last = new Date(dukasData[dukasData.length - 1].timestamp);
            
            console.log(`   First: ${first.toISOString()}`);
            console.log(`   Last:  ${last.toISOString()}`);
            
            const delayMinutes = Math.floor((now.getTime() - last.getTime()) / 60000);
            console.log(`   Delay: ${delayMinutes} minutes (last data is ${delayMinutes}m old)`);
            
            // Check coverage
            if (dukasData.length < 45) {
                console.log(`   ⚠️  Less than 45 candles for 1 hour — gaps exist in source!`);
            }
        } else {
            console.log(`   ❌ NO DATA from Dukascopy!`);
        }
        
    } catch (error) {
        console.log(`   ❌ Dukascopy error: ${error.message}`);
        
        if (error.message.includes('rate') || error.message.includes('429')) {
            console.log(`   ⚠️  RATE LIMITED! Wait 5 minutes and try again.`);
        }
    }
    
    console.log('');
    
    // ===========================================
    // 3. COMPARISON & RECOMMENDATION
    // ===========================================
    console.log('💡 RECOMMENDATION:');
    
    if (dbCandles.length === 0) {
        console.log('   Database is empty for this period!');
        console.log('   Run: node scripts/nuclear-fix.js ' + symbol + ' --hours 2');
    } else if (dbCandles.length < 50) {
        console.log('   Database has gaps. Try nuclear fix:');
        console.log('   Run: node scripts/nuclear-fix.js ' + symbol + ' --hours 2');
    } else {
        console.log('   Database looks OK. If chart still shows gaps:');
        console.log('   1. Check if app is fetching correct timeframe');
        console.log('   2. Check timezone settings in app');
        console.log('   3. Try refreshing/restarting the app');
    }
    
    console.log('═'.repeat(60));
}

async function main() {
    const symbol = process.argv[2]?.toUpperCase() || 'EURUSD';
    
    await database.connect();
    await diagnose(symbol);
    await database.disconnect();
}

main().catch(console.error);