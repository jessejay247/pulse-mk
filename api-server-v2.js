// =============================================================================
// api-server-v2.js - API Server with Self-Healing Integration
// =============================================================================

require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const database = require('./database');
const finnhub = require('./finnhub-integrated');
const { engine } = require('./self-healing-engine');
const { HealthMonitor } = require('./services/health-monitor');
const { GapDetector } = require('./services/gap-detector');

const app = express();
const PORT = process.env.PORT || 3001;
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });
const wsClients = new Map();

const healthMonitor = new HealthMonitor();
const gapDetector = new GapDetector();

const PRIMARY_PAIRS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF',
    'AUDUSD', 'USDCAD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
];

// =============================================================================
// MIDDLEWARE
// =============================================================================

app.use(express.json());
app.set('trust proxy', 1);

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type, X-API-Key');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// =============================================================================
// HEALTH & STATUS ENDPOINTS
// =============================================================================

// Basic health check
app.get('/health', async (req, res) => {
    const health = await healthMonitor.check(PRIMARY_PAIRS);
    
    res.json({
        status: health.overall,
        timestamp: new Date().toISOString(),
        wsClients: wsClients.size,
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
        issues: health.issues.length,
        finnhub: finnhub.getStatus(),
    });
});

// Detailed health report
app.get('/api/health', async (req, res) => {
    const health = await healthMonitor.check(PRIMARY_PAIRS);
    
    res.json({
        success: true,
        health: {
            overall: health.overall,
            timestamp: health.timestamp,
            issues: health.issues,
        },
        metrics: health.metrics,
        engine: engine.getStats(),
    });
});

// Data integrity report
app.get('/api/integrity', async (req, res) => {
    const days = parseInt(req.query.days) || 7;
    const symbol = req.query.symbol?.toUpperCase();
    
    const symbols = symbol ? [symbol] : PRIMARY_PAIRS;
    const results = {};
    
    for (const sym of symbols) {
        results[sym] = {
            M1: await gapDetector.fullIntegrityCheck(sym, 'M1', days),
            H1: await gapDetector.fullIntegrityCheck(sym, 'H1', days),
        };
    }
    
    res.json({
        success: true,
        period: `${days} days`,
        data: results,
    });
});

// Backfill queue status
app.get('/api/backfill-queue', async (req, res) => {
    try {
        const [rows] = await database.pool.execute(`
            SELECT symbol, timeframe, gap_start, gap_end, priority, status, attempts, error_message
            FROM pulse_backfill_queue
            ORDER BY status = 'pending' DESC, priority DESC, created_at ASC
            LIMIT 100
        `);
        
        res.json({
            success: true,
            count: rows.length,
            items: rows,
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// System stats
app.get('/api/stats', (req, res) => {
    res.json({
        success: true,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        wsClients: wsClients.size,
        finnhub: finnhub.getStatus(),
        engine: engine.getStats(),
    });
});

// =============================================================================
// DATA ENDPOINTS
// =============================================================================

// Get latest quote
app.get('/api/quote/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase().replace('/', '');
    
    try {
        const [rows] = await database.pool.execute(`
            SELECT timestamp, open, high, low, close, volume
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = 'M1'
            ORDER BY timestamp DESC
            LIMIT 1
        `, [symbol]);
        
        if (rows.length === 0) {
            return res.status(404).json({ success: false, error: 'No data' });
        }
        
        const row = rows[0];
        res.json({
            success: true,
            data: {
                symbol,
                price: parseFloat(row.close),
                bid: parseFloat(row.close),
                ask: parseFloat(row.close) * 1.00001,
                high: parseFloat(row.high),
                low: parseFloat(row.low),
                timestamp: row.timestamp,
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Get candles
app.get('/api/candles/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase().replace('/', '');
    const timeframe = (req.query.timeframe || 'H1').toUpperCase();
    const limit = Math.min(parseInt(req.query.limit) || 500, 5000);
    
    try {
        const [rows] = await database.pool.execute(`
            SELECT timestamp, open, high, low, close, volume
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT ?
        `, [symbol, timeframe, limit]);
        
        res.json({
            success: true,
            symbol,
            timeframe,
            count: rows.length,
            data: rows.reverse().map(r => ({
                t: new Date(r.timestamp).getTime(),
                o: parseFloat(r.open),
                h: parseFloat(r.high),
                l: parseFloat(r.low),
                c: parseFloat(r.close),
                v: parseFloat(r.volume),
            }))
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// =============================================================================
// WEBSOCKET
// =============================================================================

server.on('upgrade', async (request, socket, head) => {
    try {
        wss.handleUpgrade(request, socket, head, (ws) => {
            ws.subscriptions = new Set();
            ws.connectedAt = Date.now();
            wss.emit('connection', ws, request);
        });
    } catch (error) {
        console.error('WS upgrade error:', error.message);
        socket.destroy();
    }
});

wss.on('connection', (ws) => {
    const clientId = `ws-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    wsClients.set(clientId, ws);
    
    console.log(`📱 Connected: ${clientId} (total: ${wsClients.size})`);

    ws.send(JSON.stringify({
        event: 'authenticated',
        message: 'Connected to Pulse Markets Self-Healing Engine'
    }));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const action = data.action || data.type;
            
            if (action === 'subscribe') {
                const symbols = data.symbols || (data.symbol ? [data.symbol] : []);
                symbols.forEach(s => ws.subscriptions.add(s.replace('/', '')));
                ws.send(JSON.stringify({ event: 'subscribed', symbols }));
            } else if (action === 'unsubscribe') {
                ws.subscriptions.clear();
                ws.send(JSON.stringify({ event: 'unsubscribed' }));
            } else if (action === 'ping') {
                ws.send(JSON.stringify({ event: 'pong', timestamp: new Date().toISOString() }));
            }
        } catch (e) {
            ws.send(JSON.stringify({ event: 'error', message: 'Invalid message' }));
        }
    });

    ws.on('close', () => {
        wsClients.delete(clientId);
        console.log(`📱 Disconnected: ${clientId} (total: ${wsClients.size})`);
    });

    ws.on('error', () => wsClients.delete(clientId));
});

// Broadcast ticks from Finnhub
finnhub.onTick((tick) => {
    if (wsClients.size === 0) return;
    
    const message = JSON.stringify({
        event: 'quote',
        type: 'tick',
        data: {
            symbol: tick.symbol,
            displaySymbol: tick.displaySymbol,
            bid: tick.price,
            ask: tick.price * 1.00001,
            price: tick.price,
            volume: tick.volume,
            timestamp: tick.timestamp.toISOString(),
            marketClosed: tick.marketClosed,
        }
    });

    for (const [_, ws] of wsClients) {
        if (ws.readyState === 1 && ws.subscriptions.has(tick.symbol)) {
            ws.send(message);
        }
    }
});

// =============================================================================
// ERROR HANDLING
// =============================================================================

app.use((req, res) => {
    res.status(404).json({ 
        success: false, 
        error: 'Not found',
        endpoints: [
            'GET /health',
            'GET /api/health',
            'GET /api/integrity',
            'GET /api/stats',
            'GET /api/quote/:symbol',
            'GET /api/candles/:symbol',
        ]
    });
});

// =============================================================================
// STARTUP
// =============================================================================

async function start() {
    try {
        console.log('='.repeat(60));
        console.log('🚀 PulseMarkets API Server v2 (Self-Healing)');
        console.log('='.repeat(60));
        
        await database.connect();
        await finnhub.init();
        await engine.init();
        
        server.listen(PORT, '0.0.0.0', () => {
            console.log(`\n✅ Server running on port ${PORT}`);
            console.log(`📊 Memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
            console.log('='.repeat(60));
        });
    } catch (error) {
        console.error('❌ Startup failed:', error);
        process.exit(1);
    }
}

// Graceful shutdown
async function shutdown() {
    console.log('\n🛑 Shutting down...');
    await finnhub.shutdown();
    await engine.shutdown();
    await database.disconnect();
    process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start();