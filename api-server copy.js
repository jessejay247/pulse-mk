// const express = require('express');
// const database = require('./database');

// const app = express();
// const PORT = 3001; // Changed to 3001

// // Store recent ticks for real-time display
// let recentTicks = [];
// const MAX_TICKS = 50;

// // Initialize database first
// database.connect().then(() => {
//     app.use(express.json());
//     app.use(express.static('public')); // Serve static files

//     // API endpoint to get candle data for charting
//     app.get('/api/candles/:symbol/:timeframe', async (req, res) => {
//         try {
//             const { symbol, timeframe } = req.params;
//             const limit = parseInt(req.query.limit) || 1000;
            
//             const [rows] = await database.connection.execute(
//                 `SELECT timestamp, open, high, low, close, volume, spread 
//                  FROM pulse_market_data 
//                  WHERE symbol = ? AND timeframe = ? 
//                  ORDER BY timestamp DESC 
//                  LIMIT ?`,
//                 [symbol, timeframe, limit]
//             );
            
//             res.json({
//                 success: true,
//                 symbol,
//                 timeframe,
//                 data: rows.reverse()
//             });
//         } catch (error) {
//             res.status(500).json({
//                 success: false,
//                 error: error.message
//             });
//         }
//     });

//     // API endpoint to get recent ticks
//     app.get('/api/ticks', (req, res) => {
//         res.json({
//             success: true,
//             ticks: recentTicks
//         });
//     });

//     // API endpoint to get available symbols
// // In the /api/symbols endpoint, update to:
// app.get('/api/symbols', (req, res) => {
//     const symbols = {
//         // Forex
//         'EURUSD': 'OANDA:EUR_USD',
//         'GBPUSD': 'OANDA:GBP_USD', 
//         'USDJPY': 'OANDA:USD_JPY',
//         'XAUUSD': 'OANDA:XAU_USD',
//         'USDCHF': 'OANDA:USD_CHF',
//         'AUDUSD': 'OANDA:AUD_USD',
//         'USDCAD': 'OANDA:USD_CAD',
//         'NZDUSD': 'OANDA:NZD_USD',
        
//         // Crypto
//         'BTCUSD': 'BINANCE:BTCUSDT',
//         'ETHUSD': 'BINANCE:ETHUSDT',
//         'ADAUSD': 'BINANCE:ADAUSDT',
//         'DOTUSD': 'BINANCE:DOTUSDT',
//         'SOLUSD': 'BINANCE:SOLUSDT',
//         'XRPUSD': 'BINANCE:XRPUSDT',
//         'DOGEUSD': 'BINANCE:DOGEUSDT',
//         'LTCUSD': 'BINANCE:LTCUSDT',
        
//         // Stocks
//         'AAPL': 'AAPL',
//         'TSLA': 'TSLA',
//         'MSFT': 'MSFT',
//         'GOOGL': 'GOOGL',
//         'AMZN': 'AMZN',
//         'META': 'META',
//         'NVDA': 'NVDA',
//         'SPY': 'SPY'
//     };
    
//     res.json({
//         success: true,
//         symbols: Object.keys(symbols)
//     });
// });

//     // Endpoint to add ticks (called from finnhub-app.js)
//     app.post('/api/tick', (req, res) => {
//         const tick = req.body;
//         recentTicks.unshift(tick); // Add to beginning
//         if (recentTicks.length > MAX_TICKS) {
//             recentTicks = recentTicks.slice(0, MAX_TICKS);
//         }
//         res.json({ success: true });
//     });

//     app.listen(PORT, () => {
//         console.log(`🚀 Chart data API running on http://localhost:${PORT}`);
//     });
// });


//  node scripts/seed-historical.js EURUSD GBPUSD USDJPY XAUUSD USDCHF AUDUSD USDCAD NZDUSD XAUUSD --months 1 --timeframe M5
//  node scripts/seed-historical.js EURUSD GBPUSD --months 1 --timeframe M1
//  node scripts/seed-historical.js EURUSD GBPUSD --months 1 --timeframe H4
//  node scripts/seed-historical.js EURUSD GBPUSD --months 1 --timeframe H4
//  node scripts/seed-historical.js EURUSD GBPUSD --months 1 --timeframe M1



// node scripts/seed-historical.js EURGBP EURJPY GBPJPY --months 1 --timeframe D1