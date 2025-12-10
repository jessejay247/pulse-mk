// =============================================================================
// dukascopy-worker.js - Isolated child process for Dukascopy fetches
// =============================================================================
//
// CRASH-RESISTANT VERSION:
// - Catches ALL errors including uncaught exceptions
// - Always sends a response back to parent
// - Always exits cleanly (never hangs)
// - Releases ALL memory on exit
//
// DO NOT run this directly. It's spawned by server.js
// =============================================================================

const DUKASCOPY_INSTRUMENTS = {
    'EURUSD': 'eurusd', 'GBPUSD': 'gbpusd', 'USDJPY': 'usdjpy',
    'USDCHF': 'usdchf', 'AUDUSD': 'audusd', 'USDCAD': 'usdcad',
    'NZDUSD': 'nzdusd', 'EURGBP': 'eurgbp', 'EURJPY': 'eurjpy',
    'GBPJPY': 'gbpjpy', 'EURCHF': 'eurchf', 'GBPCHF': 'gbpchf',
    'AUDJPY': 'audjpy', 'EURAUD': 'euraud', 'EURCAD': 'eurcad',
    'GBPAUD': 'gbpaud', 'GBPCAD': 'gbpcad', 'AUDCAD': 'audcad',
    'AUDNZD': 'audnzd', 'NZDJPY': 'nzdjpy', 'CADJPY': 'cadjpy',
    'XAUUSD': 'xauusd', 'XAGUSD': 'xagusd',
};

// Track if we've already sent a response
let responseSent = false;

function sendResponse(data) {
    if (responseSent) return;
    responseSent = true;
    
    try {
        process.send(data);
    } catch (e) {
        // Can't send - just exit
    }
    
    // Always exit after sending response
    setTimeout(() => process.exit(0), 100);
}

async function fetchCandles(symbol, from, to) {
    const instrument = DUKASCOPY_INSTRUMENTS[symbol];
    if (!instrument) {
        return { candles: [], error: 'unsupported_symbol' };
    }

    try {
        // Dynamic require to catch import errors
        const { getHistoricalRates } = require('dukascopy-node');
        
        const data = await getHistoricalRates({
            instrument,
            dates: { from: new Date(from), to: new Date(to) },
            timeframe: 'm1',
            format: 'json',
            priceType: 'bid',
            volumes: true,
        });

        // Validate response
        if (!data || !Array.isArray(data)) {
            return { candles: [], error: 'invalid_response' };
        }

        // Extract minimal data to reduce IPC payload
        const candles = data.map(d => ({
            ts: d.timestamp,
            o: d.open,
            h: d.high,
            l: d.low,
            c: d.close,
            v: d.volume || 0,
        }));

        return { candles, error: null };
        
    } catch (error) {
        const msg = error.message || 'unknown_error';
        
        // Categorize errors
        if (msg.includes('ECONNRESET') || msg.includes('ETIMEDOUT') || msg.includes('ENOTFOUND')) {
            return { candles: [], error: 'network_error' };
        }
        if (msg.includes('429') || msg.toLowerCase().includes('rate') || msg.toLowerCase().includes('limit')) {
            return { candles: [], error: 'rate_limited' };
        }
        if (msg.includes('No data') || msg.includes('empty')) {
            return { candles: [], error: 'no_data' };
        }
        
        return { candles: [], error: msg.slice(0, 100) }; // Truncate long errors
    }
}

// =============================================================================
// MESSAGE HANDLER
// =============================================================================

process.on('message', async (msg) => {
    const { symbol, from, to } = msg;
    
    try {
        const result = await fetchCandles(symbol, from, to);
        sendResponse(result);
    } catch (error) {
        sendResponse({ candles: [], error: error.message || 'handler_error' });
    }
});

// =============================================================================
// GLOBAL ERROR HANDLERS - Prevent crashes without response
// =============================================================================

process.on('uncaughtException', (error) => {
    sendResponse({ candles: [], error: `uncaught: ${error.message}` });
});

process.on('unhandledRejection', (reason) => {
    const msg = reason instanceof Error ? reason.message : String(reason);
    sendResponse({ candles: [], error: `unhandled: ${msg}` });
});

// =============================================================================
// SAFETY TIMEOUTS
// =============================================================================

// Exit if no message received within 20 seconds
setTimeout(() => {
    sendResponse({ candles: [], error: 'no_message_timeout' });
}, 20000);

// Hard exit after 40 seconds no matter what
setTimeout(() => {
    process.exit(1);
}, 40000);