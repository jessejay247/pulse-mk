// =============================================================================
// api-server.js - Minimal WebSocket Server (Render)
// Historical data handled by Hostinger PHP
// =============================================================================

require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const database = require('./database');
const dataIngestion = require('./finnhub-app');

const app = express();
const PORT = process.env.PORT || 3001;
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });
const wsClients = new Map();

// =============================================================================
// MIDDLEWARE
// =============================================================================

app.use(express.json());
app.set('trust proxy', 1);

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type, X-API-Key');
    res.header('Access-Control-Allow-Methods', 'GET, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// =============================================================================
// SIMPLE ENDPOINTS (for debugging)
// =============================================================================

app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        mode: 'websocket-only',
        wsClients: wsClients.size,
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
        dataIngestion: dataIngestion.getStatus()
    });
});

app.get('/api/health', (req, res) => {
    res.json({
        success: true,
        status: 'healthy',
        wsClients: wsClients.size,
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB'
    });
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
    const clientId = `ws-${Date.now()}`;
    wsClients.set(clientId, ws);
    
    console.log(`📱 Connected: ${clientId} (total: ${wsClients.size})`);

    ws.send(JSON.stringify({
        event: 'authenticated',
        message: 'Connected to Pulse Markets'
    }));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const action = data.action || data.type;
            
            if (action === 'subscribe') {
                const symbols = data.symbols || (data.symbol ? [data.symbol] : []);
                symbols.forEach(s => ws.subscriptions.add(s.replace('/', '')));
                ws.send(JSON.stringify({ event: 'subscribed', symbols }));
                console.log(`📊 ${clientId} subscribed to: ${symbols.join(', ')}`);
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

// Broadcast ticks to subscribers
dataIngestion.onTick((tick) => {
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
            timestamp: tick.timestamp.toISOString()
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
        hint: 'Historical data is served from Hostinger PHP API'
    });
});

// =============================================================================
// STARTUP
// =============================================================================

async function start() {
    try {
        await database.connect();
        await dataIngestion.init();
        
        server.listen(PORT, '0.0.0.0', () => {
            console.log(`🚀 WebSocket server running on port ${PORT}`);
            console.log(`📊 Memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
            console.log(`ℹ️  Historical data served from Hostinger PHP`);
        });
    } catch (error) {
        console.error('❌ Startup failed:', error);
        process.exit(1);
    }
}

start();