// =============================================================================
// server.js - Combined API + Healing (Rate-Limited, Crash-Resistant)
// =============================================================================
//
// FIXES:
// 1. Proper rate limiting for Dukascopy (max 20 req/min)
// 2. Better error handling to prevent "Child exited with code null"
// 3. Staggered healing to avoid bursts
// 4. Automatic cooldown on rate limit detection
//
// Start: node server.js
// =============================================================================

require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cron = require('node-cron');
const { fork } = require('child_process');
const path = require('path');

const database = require('./database');
const { SYMBOLS, getSymbolByFinnhub, toInternalSymbol } = require('./config/symbols');
const { isMarketOpenForSymbol } = require('./config/market-hours');

// =============================================================================
// CONFIGURATION
// =============================================================================

const PORT = process.env.PORT || 3001;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

const CONFIG = {
    primaryPairs: [
        'EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF',
        'AUDUSD', 'USDCAD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
    ],
    secondaryPairs: [
        'XAGUSD', 'EURCHF', 'GBPCHF', 'AUDJPY', 'EURAUD',
        'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
        'NZDJPY', 'CADJPY'
    ],
    healingWindowMinutes: 15,
    dukascopyDelayMinutes: 20,
    
    // Rate limiting - CRITICAL for preventing crashes
    childTimeoutMs: 45000,           // 45 second timeout per child
    delayBetweenSymbols: 3500,       // 3.5 seconds between symbols (~17/min)
    delayBetweenTimeframes: 1500,    // 1.5 seconds between timeframes
    minHealingIntervalMs: 5 * 60 * 1000, // Minimum 5 min between healing runs
    
    // Rate limit protection
    maxConsecutiveFailures: 2,       // Pause after 2 failures
    rateLimitCooldownMs: 3 * 60 * 1000, // 3 minute cooldown on rate limit
};

// =============================================================================
// STATE
// =============================================================================

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });
const wsClients = new Map();

let finnhubWs = null;
let finnhubConnected = false;
let finnhubReconnectAttempts = 0;

// Healing state
let isHealing = false;
let lastHealingStart = 0;
let consecutiveFailures = 0;
let rateLimitedUntil = 0;

const lastHealTime = new Map();
const lastPrices = new Map();

const stats = {
    ticksReceived: 0,
    healingRuns: 0,
    candlesHealed: 0,
    childProcesses: 0,
    childFailures: 0,
    rateLimitHits: 0,
    startTime: Date.now(),
};

// =============================================================================
// UTILITIES
// =============================================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function getSymbolType(symbol) {
    if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return 'metal';
    return 'forex';
}

function log(msg, type = 'info') {
    const icons = { 
        info: '📊', success: '✅', error: '❌', heal: '🔧', 
        cron: '⏰', mem: '💾', child: '👶', rate: '🚦' 
    };
    const mem = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    console.log(`[${new Date().toISOString().slice(11,19)}] ${icons[type] || '📊'} ${msg} [${mem}MB]`);
}

// =============================================================================
// FINNHUB WEBSOCKET
// =============================================================================

function connectFinnhub() {
    if (finnhubWs) {
        finnhubWs.terminate();
        finnhubWs = null;
    }

    finnhubWs = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);

    finnhubWs.on('open', () => {
        log('Finnhub connected', 'success');
        finnhubConnected = true;
        finnhubReconnectAttempts = 0;
        
        for (const [symbol, config] of Object.entries(SYMBOLS)) {
            finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol: config.finnhub }));
        }
    });

    finnhubWs.on('message', async (data) => {
        try {
            const message = JSON.parse(data);
            if (message.type === 'trade' && message.data) {
                await processTrade(message.data[0]);
            }
        } catch (e) {}
    });

    finnhubWs.on('close', () => {
        finnhubConnected = false;
        scheduleFinnhubReconnect();
    });

    finnhubWs.on('error', () => {
        finnhubConnected = false;
    });
}

function scheduleFinnhubReconnect() {
    if (finnhubReconnectAttempts >= 10) return;
    const delay = Math.min(1000 * Math.pow(2, finnhubReconnectAttempts), 30000);
    finnhubReconnectAttempts++;
    setTimeout(connectFinnhub, delay);
}

async function processTrade(trade) {
    if (!trade) return;
    const { p: price, s: finnhubSymbol, t: timestamp, v: volume } = trade;
    
    const symbolInfo = getSymbolByFinnhub(finnhubSymbol);
    if (!symbolInfo) return;

    const { symbol, type } = symbolInfo;
    const internalSymbol = toInternalSymbol(symbol);
    const tradeTime = new Date(timestamp);

    stats.ticksReceived++;

    // Spike detection
    const lastPrice = lastPrices.get(internalSymbol);
    if (lastPrice) {
        const change = Math.abs((price - lastPrice) / lastPrice) * 100;
        if (change > (type === 'metal' ? 0.8 : 0.3)) return;
    }
    lastPrices.set(internalSymbol, price);

    // Market hours check
    const marketStatus = isMarketOpenForSymbol(type, tradeTime);
    if (!marketStatus.open) return;

    // Save candle
    const candleTime = new Date(tradeTime);
    candleTime.setUTCSeconds(0, 0);

    try {
        await database.pool.execute(`
            INSERT INTO pulse_market_data 
            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
            VALUES (?, 'M1', ?, ?, ?, ?, ?, ?, 0)
            ON DUPLICATE KEY UPDATE
            high = GREATEST(high, VALUES(high)),
            low = LEAST(low, VALUES(low)),
            close = VALUES(close),
            volume = volume + VALUES(volume)
        `, [internalSymbol, candleTime, price, price, price, price, volume || 0]);
    } catch (e) {}

    broadcastTick(internalSymbol, symbol, price, volume || 0, tradeTime);
}

function broadcastTick(symbol, displaySymbol, price, volume, timestamp) {
    if (wsClients.size === 0) return;
    
    const message = JSON.stringify({
        event: 'quote',
        data: { symbol, displaySymbol, price, bid: price, ask: price * 1.00001, volume, timestamp }
    });

    for (const [_, ws] of wsClients) {
        if (ws.readyState === WebSocket.OPEN && ws.subscriptions?.has(symbol)) {
            ws.send(message);
        }
    }
}

// =============================================================================
// CHILD PROCESS HEALING - Rate Limited
// =============================================================================

function fetchCandlesViaChild(symbol, from, to) {
    return new Promise((resolve) => {
        const child = fork(path.join(__dirname, 'dukascopy-worker.js'), [], {
            execArgv: ['--max-old-space-size=256'],
            stdio: ['pipe', 'pipe', 'pipe', 'ipc']
        });

        stats.childProcesses++;
        let resolved = false;
        
        const timeout = setTimeout(() => {
            if (!resolved) {
                resolved = true;
                log(`Child timeout for ${symbol}`, 'error');
                child.kill('SIGKILL');
                resolve({ candles: [], error: 'timeout' });
            }
        }, CONFIG.childTimeoutMs);

        child.on('message', (msg) => {
            if (!resolved) {
                resolved = true;
                clearTimeout(timeout);
                child.kill();
                resolve(msg);
            }
        });

        child.on('error', (err) => {
            if (!resolved) {
                resolved = true;
                clearTimeout(timeout);
                stats.childFailures++;
                resolve({ candles: [], error: err.message });
            }
        });

        child.on('exit', (code, signal) => {
            if (!resolved) {
                resolved = true;
                clearTimeout(timeout);
                stats.childFailures++;
                
                // Null code = killed by signal (likely rate limit crash)
                if (code === null) {
                    log(`Child killed (${signal}) for ${symbol} - possible rate limit`, 'error');
                    consecutiveFailures++;
                }
                
                resolve({ candles: [], error: `exit_${code}_${signal}` });
            }
        });

        child.send({ symbol, from: from.toISOString(), to: to.toISOString() });
    });
}

async function healSymbol(symbol) {
    const symbolType = getSymbolType(symbol);
    const marketStatus = isMarketOpenForSymbol(symbolType);
    if (!marketStatus.open) return null;

    const now = new Date();
    const to = new Date(now.getTime() - CONFIG.dukascopyDelayMinutes * 60 * 1000);
    const from = new Date(to.getTime() - CONFIG.healingWindowMinutes * 60 * 1000);
    
    const lastHeal = lastHealTime.get(symbol);
    const actualFrom = lastHeal && lastHeal > from ? lastHeal : from;
    
    if (actualFrom >= to) return { symbol, skipped: true, reason: 'already_healed' };

    try {
        const result = await fetchCandlesViaChild(symbol, actualFrom, to);
        
        // Check for errors
        if (result.error) {
            return { symbol, inserted: 0, error: result.error };
        }
        
        const candles = result.candles || [];
        
        if (candles.length === 0) {
            return { symbol, inserted: 0 };
        }

        // Success! Reset failure counter
        consecutiveFailures = 0;

        // DELETE existing
        await database.pool.execute(`
            DELETE FROM pulse_market_data
            WHERE symbol = ? AND timeframe = 'M1'
            AND timestamp >= ? AND timestamp < ?
        `, [symbol, actualFrom, to]);

        // INSERT candles
        let inserted = 0;
        for (const c of candles) {
            try {
                await database.pool.execute(`
                    INSERT INTO pulse_market_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES (?, 'M1', ?, ?, ?, ?, ?, ?, 0)
                `, [symbol, new Date(c.ts), c.o, c.h, c.l, c.c, c.v]);
                inserted++;
            } catch (e) {}
        }

        // Rebuild higher TFs
        await rebuildHigherTF(symbol, actualFrom, to);

        lastHealTime.set(symbol, to);
        stats.candlesHealed += inserted;

        return { symbol, inserted };
    } catch (error) {
        return { symbol, error: error.message };
    }
}

async function rebuildHigherTF(symbol, from, to) {
    const timeframes = ['M5', 'M15', 'M30', 'H1', 'H4', 'D1'];
    const tfMinutes = { M5: 5, M15: 15, M30: 30, H1: 60, H4: 240, D1: 1440 };

    for (const tf of timeframes) {
        try {
            const [m1Candles] = await database.pool.execute(`
                SELECT timestamp, open, high, low, close, volume
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = 'M1'
                AND timestamp >= ? AND timestamp < ?
                ORDER BY timestamp
            `, [symbol, from, to]);

            if (m1Candles.length === 0) continue;

            const periodMs = tfMinutes[tf] * 60 * 1000;
            const periods = new Map();

            for (const candle of m1Candles) {
                const ts = new Date(candle.timestamp).getTime();
                const periodStart = Math.floor(ts / periodMs) * periodMs;
                
                if (!periods.has(periodStart)) {
                    periods.set(periodStart, {
                        o: parseFloat(candle.open),
                        h: parseFloat(candle.high),
                        l: parseFloat(candle.low),
                        c: parseFloat(candle.close),
                        v: parseFloat(candle.volume || 0),
                    });
                } else {
                    const p = periods.get(periodStart);
                    p.h = Math.max(p.h, parseFloat(candle.high));
                    p.l = Math.min(p.l, parseFloat(candle.low));
                    p.c = parseFloat(candle.close);
                    p.v += parseFloat(candle.volume || 0);
                }
            }

            for (const [periodKey, agg] of periods) {
                await database.pool.execute(`
                    REPLACE INTO pulse_market_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                `, [symbol, tf, new Date(periodKey), agg.o, agg.h, agg.l, agg.c, agg.v]);
            }
        } catch (e) {}
    }
}

// =============================================================================
// HEALING ORCHESTRATION - Rate Limited
// =============================================================================

async function healPrimaryPairs() {
    // Check if we're rate limited
    if (Date.now() < rateLimitedUntil) {
        const waitSec = Math.ceil((rateLimitedUntil - Date.now()) / 1000);
        log(`Rate limited, waiting ${waitSec}s`, 'rate');
        return;
    }
    
    // Check minimum interval between healing runs
    const timeSinceLastHeal = Date.now() - lastHealingStart;
    if (timeSinceLastHeal < CONFIG.minHealingIntervalMs) {
        log(`Too soon since last heal (${Math.round(timeSinceLastHeal/1000)}s), skipping`, 'info');
        return;
    }
    
    // Check if already healing
    if (isHealing) {
        log('Healing in progress, skipping', 'info');
        return;
    }
    
    isHealing = true;
    lastHealingStart = Date.now();
    consecutiveFailures = 0;
    
    log('Healing primary pairs...', 'heal');
    stats.healingRuns++;
    
    try {
        for (const symbol of CONFIG.primaryPairs) {
            // Check for rate limit trigger
            if (consecutiveFailures >= CONFIG.maxConsecutiveFailures) {
                rateLimitedUntil = Date.now() + CONFIG.rateLimitCooldownMs;
                stats.rateLimitHits++;
                log(`Rate limit detected! Cooling down for ${CONFIG.rateLimitCooldownMs/1000}s`, 'rate');
                break;
            }
            
            const result = await healSymbol(symbol);
            
            if (result?.error) {
                log(`${symbol}: ❌ ${result.error}`, 'error');
            } else if (result?.inserted > 0) {
                log(`${symbol}: +${result.inserted}`, 'success');
            }
            
            // Rate limit: wait between symbols
            await sleep(CONFIG.delayBetweenSymbols);
        }
    } finally {
        isHealing = false;
        log('Primary healing complete', 'mem');
    }
}

async function healSecondaryPairs() {
    if (Date.now() < rateLimitedUntil || isHealing) return;
    
    isHealing = true;
    log('Healing secondary pairs...', 'heal');
    
    try {
        for (const symbol of CONFIG.secondaryPairs) {
            if (consecutiveFailures >= CONFIG.maxConsecutiveFailures) {
                rateLimitedUntil = Date.now() + CONFIG.rateLimitCooldownMs;
                stats.rateLimitHits++;
                log(`Rate limit detected! Cooling down`, 'rate');
                break;
            }
            
            const result = await healSymbol(symbol);
            
            if (result?.inserted > 0) {
                log(`${symbol}: +${result.inserted}`, 'success');
            }
            
            await sleep(CONFIG.delayBetweenSymbols);
        }
    } finally {
        isHealing = false;
        log('Secondary healing complete', 'mem');
    }
}

// =============================================================================
// EXPRESS API
// =============================================================================

app.use(express.json());

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type, X-API-Key');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

app.get('/health', (req, res) => {
    const mem = process.memoryUsage();
    res.json({
        status: 'healthy',
        uptime: Math.round((Date.now() - stats.startTime) / 1000) + 's',
        finnhub: finnhubConnected ? 'connected' : 'disconnected',
        wsClients: wsClients.size,
        memory: {
            heapUsed: Math.round(mem.heapUsed / 1024 / 1024) + 'MB',
            heapTotal: Math.round(mem.heapTotal / 1024 / 1024) + 'MB',
        },
        healing: {
            isHealing,
            rateLimited: Date.now() < rateLimitedUntil,
            rateLimitEnds: rateLimitedUntil > Date.now() ? new Date(rateLimitedUntil).toISOString() : null,
            consecutiveFailures,
        },
        stats,
    });
});

app.get('/api/quote/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase().replace('/', '');
    try {
        const [rows] = await database.pool.execute(`
            SELECT timestamp, close, high, low FROM pulse_market_data
            WHERE symbol = ? AND timeframe = 'M1'
            ORDER BY timestamp DESC LIMIT 1
        `, [symbol]);
        
        if (rows.length === 0) {
            return res.status(404).json({ error: 'No data' });
        }
        
        const row = rows[0];
        res.json({
            symbol,
            price: parseFloat(row.close),
            high: parseFloat(row.high),
            low: parseFloat(row.low),
            timestamp: row.timestamp,
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/candles/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase().replace('/', '');
    const timeframe = (req.query.timeframe || 'M1').toUpperCase();
    const limit = Math.min(parseInt(req.query.limit) || 500, 5000);
    
    try {
        const [rows] = await database.pool.execute(`
            SELECT timestamp, open, high, low, close, volume
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT ?
        `, [symbol, timeframe, limit]);
        
        res.json(rows.reverse());
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// WebSocket upgrade
server.on('upgrade', (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req);
    });
});

wss.on('connection', (ws) => {
    const clientId = Date.now().toString(36) + Math.random().toString(36).slice(2);
    ws.subscriptions = new Set();
    wsClients.set(clientId, ws);

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            if (msg.event === 'subscribe') {
                const symbols = Array.isArray(msg.data) ? msg.data : (msg.data?.symbol ? [msg.data.symbol] : []);
                symbols.forEach(s => ws.subscriptions.add(s.replace('/', '')));
                ws.send(JSON.stringify({ event: 'subscribed', symbols }));
            }
        } catch (e) {}
    });

    ws.on('close', () => wsClients.delete(clientId));
    ws.on('error', () => wsClients.delete(clientId));
});

// =============================================================================
// CRON & STARTUP
// =============================================================================

function scheduleCronJobs() {
    // Every 5 minutes: Heal primary pairs (staggered)
    cron.schedule('*/5 * * * *', () => {
        // Add random delay 0-30s to stagger across instances
        const delay = Math.random() * 30000;
        setTimeout(() => healPrimaryPairs(), delay);
    });
    log('Cron: Primary healing every 5 min', 'cron');

    // Every 15 minutes: Heal secondary pairs
    cron.schedule('*/15 * * * *', () => {
        const delay = Math.random() * 30000;
        setTimeout(() => healSecondaryPairs(), delay);
    });
    log('Cron: Secondary healing every 15 min', 'cron');

    // Finnhub heartbeat
    setInterval(() => {
        if (finnhubWs?.readyState === WebSocket.OPEN) {
            finnhubWs.send(JSON.stringify({ type: 'ping' }));
        }
    }, 30000);
}

async function start() {
    console.log('='.repeat(60));
    console.log('🚀 PulseMarkets Server (Rate-Limited Healing)');
    console.log('='.repeat(60));
    console.log(`⏱️  Delay between symbols: ${CONFIG.delayBetweenSymbols}ms`);
    console.log(`⏱️  Max ~${Math.floor(60000 / CONFIG.delayBetweenSymbols)} Dukascopy req/min`);
    console.log(`🛡️  Rate limit cooldown: ${CONFIG.rateLimitCooldownMs/1000}s`);
    console.log('='.repeat(60));
    
    await database.connect();
    connectFinnhub();
    scheduleCronJobs();
    
    server.listen(PORT, '0.0.0.0', () => {
        log(`Server on port ${PORT}`, 'success');
        log(`Primary: ${CONFIG.primaryPairs.length}, Secondary: ${CONFIG.secondaryPairs.length}`, 'info');
        console.log('='.repeat(60));
    });
}

process.on('SIGTERM', async () => {
    log('Shutting down...', 'info');
    if (finnhubWs) finnhubWs.close();
    await database.disconnect();
    process.exit(0);
});

process.on('SIGINT', async () => {
    log('Shutting down...', 'info');
    if (finnhubWs) finnhubWs.close();
    await database.disconnect();
    process.exit(0);
});

start().catch(e => {
    console.error('Fatal:', e);
    process.exit(1);
});