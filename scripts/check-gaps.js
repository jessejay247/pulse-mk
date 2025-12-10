#!/usr/bin/env node
// =============================================================================
// scripts/check-gaps.js - Quick Gap Detector
// =============================================================================
// Usage:
//   node scripts/check-gaps.js EURUSD           # Check last 2 hours
//   node scripts/check-gaps.js EURUSD --hours 6 # Check last 6 hours
//   node scripts/check-gaps.js --primary        # Check all primary pairs
// =============================================================================

require('dotenv').config();
const database = require('../database');

const PRIMARY = ['EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD'];

async function findGaps(symbol, hours = 2) {
    const to = new Date();
    const from = new Date(to.getTime() - hours * 60 * 60 * 1000);
    
    // Get all M1 candles in range
    const [rows] = await database.pool.execute(`
        SELECT timestamp
        FROM pulse_market_data
        WHERE symbol = ? AND timeframe = 'M1'
        AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    `, [symbol, from, to]);
    
    if (rows.length === 0) {
        return { symbol, gaps: [], totalCandles: 0, expectedCandles: hours * 60, coverage: 0 };
    }
    
    const gaps = [];
    const expectedInterval = 60 * 1000; // 1 minute
    
    for (let i = 1; i < rows.length; i++) {
        const prev = new Date(rows[i - 1].timestamp).getTime();
        const curr = new Date(rows[i].timestamp).getTime();
        const diff = curr - prev;
        
        // Gap if more than 1.5 minutes between candles
        if (diff > expectedInterval * 1.5) {
            const missingMinutes = Math.floor(diff / expectedInterval) - 1;
            gaps.push({
                from: new Date(prev + expectedInterval),
                to: new Date(curr),
                missingMinutes,
                ageMinutes: Math.floor((to.getTime() - curr) / 60000)
            });
        }
    }
    
    // Check for gap at the end (between last candle and now)
    const lastCandle = new Date(rows[rows.length - 1].timestamp).getTime();
    const timeSinceLast = to.getTime() - lastCandle;
    if (timeSinceLast > expectedInterval * 2) {
        gaps.push({
            from: new Date(lastCandle + expectedInterval),
            to: to,
            missingMinutes: Math.floor(timeSinceLast / expectedInterval) - 1,
            ageMinutes: 0,
            isRecent: true
        });
    }
    
    const expectedCandles = hours * 60;
    const coverage = (rows.length / expectedCandles * 100).toFixed(1);
    
    return { symbol, gaps, totalCandles: rows.length, expectedCandles, coverage };
}

async function main() {
    const args = process.argv.slice(2);
    
    let hours = 2;
    let symbols = [];
    
    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--hours' && args[i + 1]) {
            hours = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--primary') {
            symbols = [...PRIMARY];
        } else if (!args[i].startsWith('--')) {
            symbols.push(args[i].toUpperCase());
        }
    }
    
    if (symbols.length === 0) {
        symbols = ['EURUSD'];
    }
    
    await database.connect();
    
    console.log('═'.repeat(60));
    console.log(`🔍 GAP CHECK - Last ${hours} hours`);
    console.log('═'.repeat(60));
    
    for (const symbol of symbols) {
        const result = await findGaps(symbol, hours);
        
        if (result.gaps.length === 0) {
            console.log(`✅ ${symbol}: No gaps (${result.coverage}% coverage, ${result.totalCandles}/${result.expectedCandles} candles)`);
        } else {
            console.log(`\n⚠️  ${symbol}: ${result.gaps.length} gaps found (${result.coverage}% coverage)`);
            
            for (const gap of result.gaps) {
                const fromStr = gap.from.toISOString().slice(11, 16);
                const toStr = gap.to.toISOString().slice(11, 16);
                
                if (gap.isRecent) {
                    console.log(`   🔴 ${fromStr} → NOW (${gap.missingMinutes} min) ← RECENT, wait for real-time`);
                } else if (gap.ageMinutes < 20) {
                    console.log(`   🟡 ${fromStr} → ${toStr} (${gap.missingMinutes} min, ${gap.ageMinutes}m ago) ← Too recent for Dukascopy`);
                } else {
                    console.log(`   🟢 ${fromStr} → ${toStr} (${gap.missingMinutes} min, ${gap.ageMinutes}m ago) ← Can fix with Dukascopy`);
                }
            }
            
            // Suggest fix command
            const fixableGaps = result.gaps.filter(g => g.ageMinutes >= 20 && !g.isRecent);
            if (fixableGaps.length > 0) {
                console.log(`\n   💡 Fix with: node scripts/nuclear-fix.js ${symbol} --hours ${hours}`);
            }
        }
    }
    
    console.log('\n' + '═'.repeat(60));
    console.log('Legend:');
    console.log('  🔴 Recent gap - Wait for real-time feed');
    console.log('  🟡 < 20 min ago - Dukascopy doesn\'t have data yet');
    console.log('  🟢 > 20 min ago - Can fix with nuclear-fix or backfill');
    console.log('═'.repeat(60));
    
    await database.disconnect();
}

main().catch(console.error);