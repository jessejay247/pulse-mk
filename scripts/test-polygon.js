#!/usr/bin/env node
// =============================================================================
// scripts/test-polygon.js - Test Polygon.io Connection & Data
// =============================================================================
// Usage: node scripts/test-polygon.js
// =============================================================================

require('dotenv').config();

const { PolygonService } = require('../services/polygon-service');
const { UnifiedDataProvider } = require('../services/unified-data-provider');

async function testPolygon() {
    console.log('═'.repeat(60));
    console.log('🧪 POLYGON.IO CONNECTION TEST');
    console.log('═'.repeat(60));
    
    const apiKey = process.env.POLYGON_API_KEY;
    
    if (!apiKey) {
        console.log('\n❌ POLYGON_API_KEY not found in environment!\n');
        console.log('Add to your .env file:');
        console.log('   POLYGON_API_KEY=pxJc88S7IMegiPp_mqz8QB5I65pZnoTj\n');
        return;
    }
    
    console.log(`\n✅ API Key found: ${apiKey.slice(0, 8)}...${apiKey.slice(-4)}\n`);
    
    const polygon = new PolygonService(apiKey);
    
    // Test 1: Check data delay
    console.log('📊 Test 1: Checking data delay...\n');
    
    try {
        const delay = await polygon.getDataDelay('EURUSD');
        
        if (delay.error) {
            console.log(`   ❌ Error: ${delay.error}`);
        } else {
            console.log(`   ✅ Polygon data delay: ${delay.delayMinutes} minutes`);
            console.log(`   📍 Last candle: ${delay.lastCandle}`);
            
            if (delay.delayMinutes < 10) {
                console.log(`   🎉 Excellent! Polygon has near real-time data!`);
            } else if (delay.delayMinutes < 30) {
                console.log(`   👍 Good! Better than Dukascopy's typical 30-45 min delay`);
            } else {
                console.log(`   ⚠️ Higher than expected delay`);
            }
        }
    } catch (error) {
        console.log(`   ❌ Error: ${error.message}`);
    }
    
    // Test 2: Fetch sample data
    console.log('\n📊 Test 2: Fetching sample data (last 30 min)...\n');
    
    try {
        const now = new Date();
        const thirtyMinAgo = new Date(now.getTime() - 30 * 60 * 1000);
        
        const candles = await polygon.fetchCandles('EURUSD', 'M1', thirtyMinAgo, now);
        
        console.log(`   ✅ Fetched ${candles.length} M1 candles`);
        
        if (candles.length > 0) {
            const first = candles[0];
            const last = candles[candles.length - 1];
            
            console.log(`   📍 First: ${new Date(first.timestamp).toISOString()} - Close: ${first.close}`);
            console.log(`   📍 Last:  ${new Date(last.timestamp).toISOString()} - Close: ${last.close}`);
        }
    } catch (error) {
        console.log(`   ❌ Error: ${error.message}`);
        
        if (error.message.includes('auth') || error.message.includes('401')) {
            console.log(`\n   ⚠️ API Key might be invalid or expired`);
        }
    }
    
    // Test 3: Compare with Dukascopy
    console.log('\n📊 Test 3: Comparing delays (Polygon vs Dukascopy)...\n');
    
    try {
        const provider = new UnifiedDataProvider({ polygonApiKey: apiKey });
        const delays = await provider.checkDelays('EURUSD');
        
        console.log('   Source      | Delay (min) | Last Data');
        console.log('   ' + '-'.repeat(50));
        
        const polyDelay = delays.polygon.delayMinutes || 'ERROR';
        const dukaDelay = delays.dukascopy.delayMinutes || 'ERROR';
        
        console.log(`   Polygon     | ${String(polyDelay).padEnd(11)} | ${delays.polygon.lastCandle || delays.polygon.error}`);
        console.log(`   Dukascopy   | ${String(dukaDelay).padEnd(11)} | ${delays.dukascopy.lastCandle || delays.dukascopy.error}`);
        
        console.log('   ' + '-'.repeat(50));
        
        if (typeof polyDelay === 'number' && typeof dukaDelay === 'number') {
            if (polyDelay < dukaDelay) {
                console.log(`\n   🎯 Polygon is ${dukaDelay - polyDelay} minutes faster!`);
                console.log(`      Use: --source polygon-first (default)`);
            } else {
                console.log(`\n   🎯 Dukascopy is ${polyDelay - dukaDelay} minutes faster`);
                console.log(`      Use: --source dukascopy-first`);
            }
        }
    } catch (error) {
        console.log(`   ❌ Comparison error: ${error.message}`);
    }
    
    console.log('\n' + '═'.repeat(60));
    console.log('📖 USAGE');
    console.log('═'.repeat(60));
    console.log(`
Now you can use Polygon in your scripts:

  # Fix gaps with Polygon (primary) + Dukascopy (fallback)
  node scripts/nuclear-fix.js EURUSD --hours 2

  # Force Polygon only
  node scripts/nuclear-fix.js EURUSD --hours 2 --source polygon-only

  # Force Dukascopy only
  node scripts/nuclear-fix.js EURUSD --hours 2 --source dukascopy-only

  # Check current delays
  node scripts/nuclear-fix.js --check-delays
`);
    console.log('═'.repeat(60));
}

testPolygon().catch(console.error);