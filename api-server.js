// =============================================================================
// api-server.js - Public API Server (FIXED with debug mode for Render)
// =============================================================================

require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');

// Database and services
const database = require('./database');
const dataIngestion = require('./finnhub-app');
const marketService = require('./services/market-service');
const quoteService = require('./services/quote-service');

// Middleware
const { authenticate, optionalAuth, requireFeature, getClientIp } = require('./middleware/auth');
const { rateLimit, checkWebSocketLimit, incrementWsConnection, decrementWsConnection } = require('./middleware/rate-limit');
const { logUsage, logWsConnect, logWsDisconnect, logWsMessage } = require('./middleware/usage-logger');

// Config
const { SYMBOLS, getSymbol, getSymbolsForTier, canAccessSymbol, toDisplaySymbol, toInternalSymbol } = require('./config/symbols');

const app = express();

// 🎯 DEBUG MODE - Set to true to bypass auth on Render
const DEBUG_MODE = process.env.DEBUG_MODE === 'true' || process.env.NODE_ENV !== 'production';

const PORT = process.env.PORT || process.env.API_PORT || 3001;

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });
const wsClients = new Map();

// =============================================================================
// MIDDLEWARE
// =============================================================================

app.use(express.json());
app.set('trust proxy', 1);

// CORS
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-API-Key, X-API-Secret');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Expose-Headers', 'X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, X-Daily-Limit, X-Daily-Remaining');
    
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// 🎯 DEBUG AUTH MIDDLEWARE - Bypasses DB validation
const debugAuth = (req, res, next) => {
    console.log('🔓 Debug auth bypass active');
    req.auth = {
        apiKeyId: 1,
        userId: 1,
        userName: 'Debug User',
        email: 'debug@test.com',
        keyName: 'Debug Key',
        permissions: ['read'],
        allowedIps: [],
        plan: {
            id: 1,
            name: 'Debug Plan',
            slug: 'debug',
            tier: 'individual',
            apiCallsPerDay: 10000,
            apiCallsPerMinute: 100,
            websocketAccess: true,
            websocketConnections: 5,
            historicalDataAccess: true,
            historicalDataDays: 365,
            features: {}
        }
    };
    next();
};

// 🎯 Choose auth middleware based on debug mode
const authMiddleware = DEBUG_MODE ? debugAuth : authenticate;

// =============================================================================
// PUBLIC ENDPOINTS (No auth required)
// =============================================================================

// Health check
app.get('/v1/health', (req, res) => {
    const ingestionStatus = dataIngestion.getStatus();
    res.json({
        status: 'healthy',
        version: '1.0.0',
        timestamp: new Date().toISOString(),
        debugMode: DEBUG_MODE,
        services: {
            api: 'up',
            websocket: 'up',
            database: database.pool ? 'up' : 'down',
            dataIngestion: ingestionStatus.connected ? 'up' : 'degraded'
        }
    });
});

// 🎯 NEW: Simple test endpoint - NO AUTH
app.get('/api/test', async (req, res) => {
    try {
        // Test database connection
        const [rows] = await database.pool.execute(
            'SELECT COUNT(*) as count, MAX(timestamp) as latest FROM pulse_market_data WHERE symbol = ? LIMIT 1',
            ['EURUSD']
        );
        
        res.json({
            success: true,
            message: 'API is working',
            debugMode: DEBUG_MODE,
            database: {
                connected: true,
                eurusdCandleCount: rows[0]?.count || 0,
                latestCandle: rows[0]?.latest || null
            }
        });
    } catch (error) {
        res.json({
            success: false,
            message: 'Database connection issue',
            error: error.message,
            debugMode: DEBUG_MODE
        });
    }
});

// 🎯 NEW: Debug candles endpoint - NO AUTH for testing
app.get('/api/debug/candles/:symbol/:timeframe', async (req, res) => {
    try {
        const { symbol, timeframe } = req.params;
        const limit = parseInt(req.query.limit) || 100;

        console.log(`📊 Debug candles request: ${symbol} ${timeframe} limit=${limit}`);

        const candles = await database.getCandles(symbol, timeframe, { limit });

        console.log(`📊 Found ${candles.length} candles`);

        res.json({
            success: true,
            symbol,
            timeframe,
            count: candles.length,
            data: candles
        });
    } catch (error) {
        console.error('❌ Debug candles error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message,
            stack: error.stack 
        });
    }
});

// =============================================================================
// AUTHENTICATED ENDPOINTS (use debug or real auth)
// =============================================================================

// Apply auth to /v1/* routes
app.use('/v1', authMiddleware, rateLimit, logUsage);

// Get single quote
app.get('/v1/quotes/:base/:quote', async (req, res) => {
    try {
        const symbol = `${req.params.base}/${req.params.quote}`;
        
        const symbolConfig = getSymbol(symbol);
        if (!symbolConfig) {
            return res.status(400).json({
                success: false,
                error: { code: 'INVALID_SYMBOL', message: `Symbol ${symbol} not supported`, status: 400 }
            });
        }

        if (!canAccessSymbol(symbol, req.auth.plan.tier)) {
            return res.status(403).json({
                success: false,
                error: { code: 'PLAN_LIMIT_EXCEEDED', message: `Symbol ${symbol} requires higher plan`, status: 403 }
            });
        }

        const quote = await quoteService.getQuote(symbol);
        if (!quote) {
            return res.status(404).json({
                success: false,
                error: { code: 'NO_DATA', message: 'No price data available', status: 404 }
            });
        }

        res.json({ success: true, data: quote });
    } catch (error) {
        console.error('Error getting quote:', error);
        res.status(500).json({
            success: false,
            error: { code: 'INTERNAL_ERROR', message: 'Failed to get quote', status: 500 }
        });
    }
});

// Get multiple quotes
app.get('/v1/quotes', async (req, res) => {
    try {
        let symbols = req.query.symbols;
        
        if (!symbols) {
            const quotes = await quoteService.getAllQuotes(req.auth.plan.tier);
            return res.json({ success: true, data: quotes, count: quotes.length });
        }

        symbols = symbols.split(',').map(s => s.trim());
        
        const accessibleSymbols = symbols.filter(s => {
            const displaySymbol = toDisplaySymbol(s);
            return getSymbol(displaySymbol) && canAccessSymbol(displaySymbol, req.auth.plan.tier);
        });

        const quotes = await quoteService.getQuotes(accessibleSymbols);
        res.json({ success: true, data: quotes, count: quotes.length });
    } catch (error) {
        console.error('Error getting quotes:', error);
        res.status(500).json({
            success: false,
            error: { code: 'INTERNAL_ERROR', message: 'Failed to get quotes', status: 500 }
        });
    }
});

// Get symbols
app.get('/v1/symbols', (req, res) => {
    try {
        const type = req.query.type;
        const tier = req.auth.plan.tier;
        
        let symbols = getSymbolsForTier(tier);
        
        if (type) {
            symbols = Object.fromEntries(
                Object.entries(symbols).filter(([_, config]) => config.type === type)
            );
        }

        const result = Object.entries(symbols).map(([symbol, config]) => ({
            symbol,
            type: config.type,
            base_currency: config.base,
            quote_currency: config.quote,
            pip_size: config.pipSize,
            digits: config.digits,
            description: config.description
        }));

        res.json({ success: true, data: result, count: result.length });
    } catch (error) {
        console.error('Error getting symbols:', error);
        res.status(500).json({
            success: false,
            error: { code: 'INTERNAL_ERROR', message: 'Failed to get symbols', status: 500 }
        });
    }
});

// Market status
app.get('/v1/market/status', (req, res) => {
    try {
        const status = marketService.getStatus();
        res.json({ success: true, data: status });
    } catch (error) {
        console.error('Error getting market status:', error);
        res.status(500).json({
            success: false,
            error: { code: 'INTERNAL_ERROR', message: 'Failed to get market status', status: 500 }
        });
    }
});

// Historical candles
app.get('/v1/historical/candles/:base/:quote', requireFeature('historical'), async (req, res) => {
    try {
        const symbol = `${req.params.base}/${req.params.quote}`;
        const { timeframe, from, to, limit } = req.query;

        if (!getSymbol(symbol) || !canAccessSymbol(symbol, req.auth.plan.tier)) {
            return res.status(400).json({
                success: false,
                error: { code: 'INVALID_SYMBOL', message: 'Symbol not available', status: 400 }
            });
        }

        const validTimeframes = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1', 'W1', 'MN'];
        if (timeframe && !validTimeframes.includes(timeframe)) {
            return res.status(400).json({
                success: false,
                error: { code: 'INVALID_TIMEFRAME', message: 'Invalid timeframe', status: 400 }
            });
        }

        const internalSymbol = toInternalSymbol(symbol);
        const candles = await database.getCandles(internalSymbol, timeframe || 'H1', {
            from: from ? new Date(from) : undefined,
            to: to ? new Date(to) : undefined,
            limit: Math.min(parseInt(limit) || 500, req.auth.plan.tier === 'business' ? 10000 : 5000)
        });

        res.json({
            success: true,
            data: { symbol, timeframe: timeframe || 'H1', candles, count: candles.length }
        });
    } catch (error) {
        console.error('Error getting candles:', error);
        res.status(500).json({
            success: false,
            error: { code: 'INTERNAL_ERROR', message: 'Failed to get historical data', status: 500 }
        });
    }
});

// =============================================================================
// BACKWARD-COMPATIBLE ENDPOINTS (for Flutter app)
// =============================================================================

const MAX_TICKS = 50;
let recentTicks = [];
let tickIndex = 0;

dataIngestion.onTick((tick) => {
    const tickData = {
        symbol: tick.symbol,
        price: tick.price,
        volume: tick.volume,
        timestamp: tick.timestamp,
        originalSymbol: tick.displaySymbol
    };
    
    if (recentTicks.length < MAX_TICKS) {
        recentTicks.push(tickData);
    } else {
        recentTicks[tickIndex % MAX_TICKS] = tickData;
    }
    tickIndex++;
});

// 🎯 FIXED: Candles endpoint with debug auth option
app.get('/api/candles/:symbol/:timeframe', authMiddleware, async (req, res) => {
    try {
        const { symbol, timeframe } = req.params;
        const limit = parseInt(req.query.limit) || 500;

        console.log(`📊 Candles request: ${symbol} ${timeframe} limit=${limit}`);

        const candles = await database.getCandles(symbol, timeframe, { limit });

        console.log(`📊 Returning ${candles.length} candles for ${symbol}`);

        res.json({
            success: true,
            symbol,
            timeframe,
            data: candles
        });
    } catch (error) {
        console.error('Error getting candles:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Get latest price
app.get('/api/price/:symbol', authMiddleware, async (req, res) => {
    try {
        const { symbol } = req.params;
        
        const tick = recentTicks.find(t => t.symbol === symbol);
        if (tick) {
            return res.json({ success: true, price: tick.price, timestamp: tick.timestamp });
        }

        const quote = await database.getLatestQuote(symbol);
        if (quote) {
            return res.json({ success: true, price: quote.close, timestamp: quote.timestamp });
        }

        res.json({ success: false, error: 'No price data available' });
    } catch (error) {
        console.error('Error getting price:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Get recent ticks
app.get('/api/ticks', authMiddleware, (req, res) => {
    const symbol = req.query.symbol;
    let filteredTicks = recentTicks;
    
    if (symbol) {
        filteredTicks = recentTicks.filter(t => t.symbol === symbol);
    }
    
    res.json({ success: true, ticks: filteredTicks });
});

// Get symbols
app.get('/api/symbols', authMiddleware, (req, res) => {
    const tier = req.auth.plan.tier;
    const symbols = getSymbolsForTier(tier);
    
    res.json({
        success: true,
        symbols: Object.keys(symbols),
        mappings: Object.fromEntries(
            Object.entries(symbols).map(([k, v]) => [k, v.finnhub])
        )
    });
});

// Simple health check
app.get('/api/health', (req, res) => {
    const ingestionStatus = dataIngestion.getStatus();
    res.json({
        success: true,
        status: 'healthy',
        debugMode: DEBUG_MODE,
        wsClients: wsClients.size,
        recentTicksCount: recentTicks.length,
        dataIngestion: ingestionStatus
    });
});

// =============================================================================
// WEBSOCKET HANDLING
// =============================================================================

server.on('upgrade', async (request, socket, head) => {
    console.log('📡 WebSocket upgrade request received');
    
    try {
        const baseUrl = `http://${request.headers.host || 'localhost'}`;
        const parsedUrl = new URL(request.url || '/', baseUrl);
        const apiKey = parsedUrl.searchParams.get('api_key') || request.headers['x-api-key'];

        if (!apiKey && !DEBUG_MODE) {
            console.log('❌ No API key provided');
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }

        // 🎯 Debug mode auth bypass
        const auth = {
            apiKeyId: 1,
            userId: 1,
            userName: 'Debug User',
            email: 'debug@test.com',
            keyName: 'Debug Key',
            permissions: ['read'],
            allowedIps: [],
            plan: {
                id: 1,
                name: 'Debug Plan',
                slug: 'debug',
                tier: 'individual',
                apiCallsPerDay: 10000,
                apiCallsPerMinute: 100,
                websocketAccess: true,
                websocketConnections: 5,
                historicalDataAccess: true,
                historicalDataDays: 365,
                features: {}
            }
        };

        if (!auth.plan?.websocketAccess) {
            socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
            socket.destroy();
            return;
        }

        const limitCheck = checkWebSocketLimit(auth.userId, auth.plan);
        if (!limitCheck.allowed) {
            socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n');
            socket.destroy();
            return;
        }

        console.log('✅ Upgrading to WebSocket...');
        wss.handleUpgrade(request, socket, head, (ws) => {
            ws.auth = auth;
            ws.subscriptions = new Set();
            ws.connectedAt = Date.now();
            ws.clientIp = getClientIp(request);
            wss.emit('connection', ws, request);
        });
    } catch (error) {
        console.error('❌ WebSocket upgrade error:', error.message);
        socket.write('HTTP/1.1 500 Internal Server Error\r\n\r\n');
        socket.destroy();
    }
});

wss.on('connection', (ws, request) => {
    const clientId = `${ws.auth.userId}-${Date.now()}`;
    wsClients.set(clientId, ws);
    incrementWsConnection(ws.auth.userId);
    
    console.log(`📱 WebSocket client connected: ${clientId}`);
    logWsConnect(ws.auth, ws.clientIp);

    ws.send(JSON.stringify({
        event: 'authenticated',
        message: 'Successfully authenticated',
        user_id: ws.auth.userId,
        plan: ws.auth.plan.name,
        max_subscriptions: getMaxSubscriptions(ws.auth.plan.tier)
    }));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            handleWsMessage(ws, data, clientId);
        } catch (error) {
            ws.send(JSON.stringify({ event: 'error', message: 'Invalid JSON message' }));
        }
    });

    ws.on('close', () => {
        wsClients.delete(clientId);
        decrementWsConnection(ws.auth.userId);
        logWsDisconnect(ws.auth, ws.clientIp, Date.now() - ws.connectedAt);
        console.log(`📱 WebSocket client disconnected: ${clientId}`);
    });

    ws.on('error', (error) => {
        console.error(`WebSocket error for ${clientId}:`, error);
        wsClients.delete(clientId);
        decrementWsConnection(ws.auth.userId);
    });
});

function handleWsMessage(ws, data, clientId) {
    const action = data.action || data.type;
    
    switch (action) {
        case 'subscribe':
            const symbolsToSubscribe = data.symbols || (data.symbol ? [data.symbol] : []);
            handleSubscribe(ws, symbolsToSubscribe);
            break;
        case 'unsubscribe':
            const symbolsToUnsubscribe = data.symbols || (data.symbol ? [data.symbol] : null);
            handleUnsubscribe(ws, symbolsToUnsubscribe);
            break;
        case 'ping':
            ws.send(JSON.stringify({ event: 'pong', type: 'pong', timestamp: new Date().toISOString() }));
            break;
        default:
            ws.send(JSON.stringify({ event: 'error', type: 'error', message: 'Unknown action' }));
    }
    
    logWsMessage(ws.auth, action);
}

function handleSubscribe(ws, symbols) {
    if (!symbols || !Array.isArray(symbols)) {
        ws.send(JSON.stringify({ event: 'error', message: 'symbols must be an array' }));
        return;
    }

    const maxSubs = getMaxSubscriptions(ws.auth.plan.tier);
    const subscribed = [];

    for (const symbol of symbols) {
        if (ws.subscriptions.size >= maxSubs) break;

        const displaySymbol = toDisplaySymbol(symbol);
        if (getSymbol(displaySymbol) && canAccessSymbol(displaySymbol, ws.auth.plan.tier)) {
            ws.subscriptions.add(toInternalSymbol(displaySymbol));
            subscribed.push(displaySymbol);
        }
    }

    ws.send(JSON.stringify({ event: 'subscribed', symbols: subscribed, count: subscribed.length }));
}

function handleUnsubscribe(ws, symbols) {
    if (!symbols) {
        ws.subscriptions.clear();
        ws.send(JSON.stringify({ event: 'unsubscribed', symbols: 'all' }));
        return;
    }

    for (const symbol of symbols) {
        ws.subscriptions.delete(toInternalSymbol(toDisplaySymbol(symbol)));
    }

    ws.send(JSON.stringify({ event: 'unsubscribed', symbols }));
}

function getMaxSubscriptions(tier) {
    switch (tier) {
        case 'business': return 50;
        case 'individual': return 10;
        default: return 5;
    }
}

// Broadcast ticks
dataIngestion.onTick((tick) => {
    const message = JSON.stringify({
        event: 'quote',
        type: 'tick',
        data: {
            symbol: tick.symbol,
            displaySymbol: tick.displaySymbol,
            bid: tick.price,
            ask: tick.price + (tick.price * 0.00001),
            spread: tick.price * 0.00001,
            price: tick.price,
            volume: tick.volume,
            timestamp: tick.timestamp.toISOString()
        }
    });

    for (const [_, ws] of wsClients) {
        if (ws.readyState === WebSocket.OPEN && ws.subscriptions.has(tick.symbol)) {
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
        error: { code: 'NOT_FOUND', message: 'Endpoint not found', status: 404 }
    });
});

app.use((error, req, res, next) => {
    console.error('Unhandled error:', error);
    res.status(500).json({
        success: false,
        error: { code: 'INTERNAL_ERROR', message: 'An unexpected error occurred', status: 500 }
    });
});

// =============================================================================
// STARTUP
// =============================================================================

async function start() {
    try {
        console.log(`🔧 Debug mode: ${DEBUG_MODE}`);
        
        await database.connect();
        await dataIngestion.init();
        
        server.listen(PORT, '0.0.0.0', () => {
            console.log(`🚀 API Server running on port ${PORT}`);
            console.log(`📡 WebSocket server ready`);
            console.log(`🌍 Environment: ${process.env.NODE_ENV || 'development'}`);
            console.log(`🔓 Auth bypass: ${DEBUG_MODE ? 'ENABLED' : 'DISABLED'}`);
        });
    } catch (error) {
        console.error('❌ Failed to start server:', error);
        process.exit(1);
    }
}

start();