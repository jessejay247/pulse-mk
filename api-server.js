// =============================================================================
// api-server.js - Public API Server
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

// Use PORT for Render compatibility (Render sets PORT, not API_PORT)
const PORT = process.env.PORT || process.env.API_PORT || 3001;

// Create HTTP server
const server = http.createServer(app);

// WebSocket server for authenticated clients
const wss = new WebSocket.Server({ noServer: true });

// Store WebSocket clients
const wsClients = new Map();

// =============================================================================
// MIDDLEWARE
// =============================================================================

app.use(express.json());

// Trust proxy for Render (important for getting real client IPs)
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
        services: {
            api: 'up',
            websocket: 'up',
            database: database.pool ? 'up' : 'down',
            dataIngestion: ingestionStatus.connected ? 'up' : 'degraded'
        }
    });
});

// =============================================================================
// AUTHENTICATED ENDPOINTS
// =============================================================================

// Apply auth, rate limiting, and logging to /v1/* routes
app.use('/v1', authenticate, rateLimit, logUsage);

// -----------------------------------------------------------------------------
// QUOTES
// -----------------------------------------------------------------------------

// Get single quote
app.get('/v1/quotes/:base/:quote', async (req, res) => {
    try {
        const symbol = `${req.params.base}/${req.params.quote}`;
        
        // Check if symbol exists
        const symbolConfig = getSymbol(symbol);
        if (!symbolConfig) {
            return res.status(400).json({
                success: false,
                error: {
                    code: 'INVALID_SYMBOL',
                    message: `The symbol ${symbol} is not supported`,
                    status: 400
                }
            });
        }

        // Check tier access
        if (!canAccessSymbol(symbol, req.auth.plan.tier)) {
            return res.status(403).json({
                success: false,
                error: {
                    code: 'PLAN_LIMIT_EXCEEDED',
                    message: `Symbol ${symbol} requires a higher plan`,
                    status: 403
                }
            });
        }

        const quote = await quoteService.getQuote(symbol);
        if (!quote) {
            return res.status(404).json({
                success: false,
                error: {
                    code: 'NO_DATA',
                    message: 'No price data available for this symbol',
                    status: 404
                }
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
            // Return all available for user's tier
            const quotes = await quoteService.getAllQuotes(req.auth.plan.tier);
            return res.json({ success: true, data: quotes, count: quotes.length });
        }

        symbols = symbols.split(',').map(s => s.trim());
        
        // Filter to accessible symbols
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

// -----------------------------------------------------------------------------
// SYMBOLS
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// CONVERT
// -----------------------------------------------------------------------------

app.get('/v1/convert', async (req, res) => {
    try {
        const { from, to, amount } = req.query;

        if (!from || !to || !amount) {
            return res.status(400).json({
                success: false,
                error: {
                    code: 'MISSING_PARAMETER',
                    message: 'from, to, and amount parameters are required',
                    status: 400
                }
            });
        }

        const numAmount = parseFloat(amount);
        if (isNaN(numAmount) || numAmount <= 0) {
            return res.status(400).json({
                success: false,
                error: {
                    code: 'INVALID_PARAMETER',
                    message: 'amount must be a positive number',
                    status: 400
                }
            });
        }

        const result = await quoteService.convert(from.toUpperCase(), to.toUpperCase(), numAmount);
        
        if (!result) {
            return res.status(400).json({
                success: false,
                error: {
                    code: 'CONVERSION_FAILED',
                    message: `Cannot convert ${from} to ${to}`,
                    status: 400
                }
            });
        }

        res.json({ success: true, data: result });
    } catch (error) {
        console.error('Error converting:', error);
        res.status(500).json({
            success: false,
            error: { code: 'INTERNAL_ERROR', message: 'Conversion failed', status: 500 }
        });
    }
});

// -----------------------------------------------------------------------------
// MARKET STATUS
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// HISTORICAL DATA
// -----------------------------------------------------------------------------

app.get('/v1/historical/candles/:base/:quote', requireFeature('historical'), async (req, res) => {
    try {
        const symbol = `${req.params.base}/${req.params.quote}`;
        const { timeframe, from, to, limit } = req.query;

        // Validate symbol
        if (!getSymbol(symbol) || !canAccessSymbol(symbol, req.auth.plan.tier)) {
            return res.status(400).json({
                success: false,
                error: { code: 'INVALID_SYMBOL', message: 'Symbol not available', status: 400 }
            });
        }

        // Validate timeframe
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
            data: {
                symbol,
                timeframe: timeframe || 'H1',
                candles,
                count: candles.length
            }
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
// WEBSOCKET HANDLING (Fixed for Render compatibility)
// =============================================================================

// Handle WebSocket upgrade using WHATWG URL API (fixes deprecation warning)
server.on('upgrade', async (request, socket, head) => {
    try {
        // Use WHATWG URL API instead of deprecated url.parse()
        const baseUrl = `http://${request.headers.host}`;
        const parsedUrl = new URL(request.url, baseUrl);
        const apiKey = parsedUrl.searchParams.get('api_key') || request.headers['x-api-key'];

        if (!apiKey) {
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }

        // Validate API key
        const auth = await database.validateApiKey(apiKey);
        if (!auth) {
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }

        // Check WebSocket access
        if (!auth.plan.websocketAccess) {
            socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
            socket.destroy();
            return;
        }

        // Check connection limit
        const limitCheck = checkWebSocketLimit(auth.userId, auth.plan);
        if (!limitCheck.allowed) {
            socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n');
            socket.destroy();
            return;
        }

        // Accept connection
        wss.handleUpgrade(request, socket, head, (ws) => {
            ws.auth = auth;
            ws.subscriptions = new Set();
            ws.connectedAt = Date.now();
            ws.clientIp = getClientIp(request);
            
            wss.emit('connection', ws, request);
        });
    } catch (error) {
        console.error('WebSocket upgrade error:', error);
        socket.write('HTTP/1.1 500 Internal Server Error\r\n\r\n');
        socket.destroy();
    }
});

// WebSocket connection handler
wss.on('connection', (ws, request) => {
    const clientId = `${ws.auth.userId}-${Date.now()}`;
    wsClients.set(clientId, ws);
    incrementWsConnection(ws.auth.userId);
    
    console.log(`📱 WebSocket client connected: ${clientId}`);
    logWsConnect(ws.auth, ws.clientIp);

    // Send auth success
    ws.send(JSON.stringify({
        event: 'authenticated',
        message: 'Successfully authenticated',
        user_id: ws.auth.userId,
        plan: ws.auth.plan.name,
        max_subscriptions: getMaxSubscriptions(ws.auth.plan.tier)
    }));

    // Message handler
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            handleWsMessage(ws, data, clientId);
        } catch (error) {
            ws.send(JSON.stringify({
                event: 'error',
                message: 'Invalid JSON message'
            }));
        }
    });

    // Close handler
    ws.on('close', () => {
        wsClients.delete(clientId);
        decrementWsConnection(ws.auth.userId);
        logWsDisconnect(ws.auth, ws.clientIp, Date.now() - ws.connectedAt);
        console.log(`📱 WebSocket client disconnected: ${clientId}`);
    });

    // Error handler
    ws.on('error', (error) => {
        console.error(`WebSocket error for ${clientId}:`, error);
        wsClients.delete(clientId);
        decrementWsConnection(ws.auth.userId);
    });
});

function handleWsMessage(ws, data, clientId) {
    // Support both 'action' (new) and 'type' (old) message formats
    const action = data.action || data.type;
    
    switch (action) {
        case 'subscribe':
            // Support both 'symbols' array (new) and 'symbol' string (old)
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
        if (ws.subscriptions.size >= maxSubs) {
            break;
        }

        const displaySymbol = toDisplaySymbol(symbol);
        if (getSymbol(displaySymbol) && canAccessSymbol(displaySymbol, ws.auth.plan.tier)) {
            ws.subscriptions.add(toInternalSymbol(displaySymbol));
            subscribed.push(displaySymbol);
        }
    }

    ws.send(JSON.stringify({
        event: 'subscribed',
        symbols: subscribed,
        count: subscribed.length
    }));
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

// Broadcast ticks to WebSocket clients
dataIngestion.onTick((tick) => {
    // Send BOTH internal symbol (BTCUSD) and display symbol (BTC/USD) for compatibility
    const message = JSON.stringify({
        event: 'quote',      // New format
        type: 'tick',        // Old format for backward compatibility
        data: {
            symbol: tick.symbol,           // Internal format: "BTCUSD" - for Flutter comparison
            displaySymbol: tick.displaySymbol, // Display format: "BTC/USD"
            bid: tick.price,
            ask: tick.price + (tick.price * 0.00001), // Small spread
            spread: tick.price * 0.00001,
            price: tick.price,             // Old format field
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
// BACKWARD-COMPATIBLE ENDPOINTS (for Flutter app)
// =============================================================================

// Store recent ticks in memory for /api/ticks endpoint
// Use a fixed-size circular buffer to prevent memory growth
const MAX_TICKS = 50; // Reduced from 100
let recentTicks = [];
let tickIndex = 0;

// Listen to data ingestion ticks and store them (memory-efficient circular buffer)
dataIngestion.onTick((tick) => {
    const tickData = {
        symbol: tick.symbol,
        price: tick.price,
        volume: tick.volume,
        timestamp: tick.timestamp,
        originalSymbol: tick.displaySymbol
    };
    
    // Circular buffer - overwrites old entries instead of growing
    if (recentTicks.length < MAX_TICKS) {
        recentTicks.push(tickData);
    } else {
        recentTicks[tickIndex % MAX_TICKS] = tickData;
    }
    tickIndex++;
});

// GET /api/candles/:symbol/:timeframe - Backward compatible candles endpoint
app.get('/api/candles/:symbol/:timeframe', authenticate, rateLimit, async (req, res) => {
    try {
        const { symbol, timeframe } = req.params;
        const limit = parseInt(req.query.limit) || 500;

        const candles = await database.getCandles(symbol, timeframe, { limit });

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

// GET /api/price/:symbol - Get latest price
app.get('/api/price/:symbol', authenticate, rateLimit, async (req, res) => {
    try {
        const { symbol } = req.params;
        
        // Check recent ticks first
        const tick = recentTicks.find(t => t.symbol === symbol);
        if (tick) {
            return res.json({ 
                success: true, 
                price: tick.price, 
                timestamp: tick.timestamp 
            });
        }

        // Fallback to database
        const quote = await database.getLatestQuote(symbol);
        if (quote) {
            return res.json({ 
                success: true, 
                price: quote.close, 
                timestamp: quote.timestamp 
            });
        }

        res.json({ success: false, error: 'No price data available' });
    } catch (error) {
        console.error('Error getting price:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /api/ticks - Get recent ticks
app.get('/api/ticks', authenticate, rateLimit, (req, res) => {
    const symbol = req.query.symbol;
    let filteredTicks = recentTicks;
    
    if (symbol) {
        filteredTicks = recentTicks.filter(t => t.symbol === symbol);
    }
    
    res.json({ success: true, ticks: filteredTicks });
});

// GET /api/symbols - Get available symbols
app.get('/api/symbols', authenticate, rateLimit, (req, res) => {
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

// GET /api/health - Simple health check (no auth)
app.get('/api/health', (req, res) => {
    const ingestionStatus = dataIngestion.getStatus();
    res.json({
        success: true,
        status: 'healthy',
        wsClients: wsClients.size,
        recentTicksCount: recentTicks.length,
        dataIngestion: ingestionStatus
    });
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
        await database.connect();
        await dataIngestion.init();
        
        server.listen(PORT, '0.0.0.0', () => {
            console.log(`🚀 API Server running on port ${PORT}`);
            console.log(`📡 WebSocket server ready`);
            console.log(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
        });
    } catch (error) {
        console.error('❌ Failed to start server:', error);
        process.exit(1);
    }
}

start();