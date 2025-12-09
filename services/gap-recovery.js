// =============================================================================
// services/gap-recovery.js - Gap Detection and Recovery Service
// =============================================================================

const axios = require('axios');
const database = require('../database');
const { SYMBOLS, toInternalSymbol } = require('../config/symbols');
const { isMarketOpenForSymbol } = require('../config/market-hours');

class GapRecoveryService {
    constructor() {
        this.finnhubApiKey = process.env.FINNHUB_API_KEY || 'd4rljk9r01qgts2osudgd4rljk9r01qgts2osue0';
        this.baseUrl = 'https://finnhub.io/api/v1';
        this.isRecovering = false;
    }

    /**
     * Check for gaps and recover missing data on startup
     */
    async recoverOnStartup() {
        if (this.isRecovering) {
            console.log('⏳ Gap recovery already in progress...');
            return;
        }

        this.isRecovering = true;
        console.log('🔍 Checking for data gaps...');

        try {
            const gaps = await this.detectGaps();
            
            if (gaps.length === 0) {
                console.log('✅ No significant gaps detected');
                return;
            }

            console.log(`📊 Found ${gaps.length} symbols with gaps, recovering...`);
            
            for (const gap of gaps) {
                await this.recoverGap(gap);
                // Rate limit: Finnhub free tier has 60 calls/minute
                await this.sleep(1500);
            }

            console.log('✅ Gap recovery complete');
        } catch (error) {
            console.error('❌ Gap recovery failed:', error);
        } finally {
            this.isRecovering = false;
        }
    }

    /**
     * Detect gaps in candle data
     * Returns list of symbols with gaps and their details
     */
    async detectGaps() {
        const gaps = [];
        const now = new Date();
        const maxGapMinutes = 15; // Consider it a gap if more than 15 minutes old

        for (const [symbol, config] of Object.entries(SYMBOLS)) {
            const internalSymbol = toInternalSymbol(symbol);
            
            // Skip if market is closed for this type
            const marketStatus = isMarketOpenForSymbol(config.type);
            if (!marketStatus.open) continue;

            const latestTime = await database.getLatestCandleTime(internalSymbol, 'M1');
            
            if (!latestTime) {
                // No data at all - big gap
                gaps.push({
                    symbol,
                    internalSymbol,
                    finnhubSymbol: config.finnhub,
                    type: config.type,
                    latestTime: null,
                    gapMinutes: null, // Unknown
                    needsFullRecovery: true
                });
                continue;
            }

            const gapMinutes = (now - new Date(latestTime)) / (1000 * 60);
            
            if (gapMinutes > maxGapMinutes) {
                gaps.push({
                    symbol,
                    internalSymbol,
                    finnhubSymbol: config.finnhub,
                    type: config.type,
                    latestTime,
                    gapMinutes: Math.round(gapMinutes),
                    needsFullRecovery: gapMinutes > 60 * 24 // More than 24 hours
                });
            }
        }

        return gaps;
    }

    /**
     * Recover data for a gap
     */
    async recoverGap(gap) {
        console.log(`📥 Recovering ${gap.symbol} (${gap.gapMinutes || 'unknown'} minutes gap)...`);

        try {
            // For forex/crypto with gaps, fetch historical candles from Finnhub
            // Note: Finnhub candles endpoint is limited on free tier
            const candles = await this.fetchHistoricalCandles(
                gap.finnhubSymbol,
                gap.latestTime,
                gap.type
            );

            if (candles && candles.length > 0) {
                await this.saveCandles(gap.internalSymbol, candles);
                console.log(`✅ Recovered ${candles.length} candles for ${gap.symbol}`);
            } else {
                console.log(`⚠️ No historical data available for ${gap.symbol}`);
            }
        } catch (error) {
            console.error(`❌ Failed to recover ${gap.symbol}:`, error.message);
        }
    }

    /**
     * Fetch historical candles from Finnhub
     */
    async fetchHistoricalCandles(finnhubSymbol, fromTime, symbolType) {
        try {
            // Calculate time range
            const from = fromTime ? Math.floor(new Date(fromTime).getTime() / 1000) : 
                         Math.floor((Date.now() - 24 * 60 * 60 * 1000) / 1000);
            const to = Math.floor(Date.now() / 1000);

            // Determine resolution based on gap size
            const resolution = '1'; // 1 minute candles

            let url;
            if (symbolType === 'crypto') {
                // Crypto uses different endpoint
                url = `${this.baseUrl}/crypto/candle?symbol=${finnhubSymbol}&resolution=${resolution}&from=${from}&to=${to}&token=${this.finnhubApiKey}`;
            } else if (symbolType === 'forex' || symbolType === 'metal') {
                url = `${this.baseUrl}/forex/candle?symbol=${finnhubSymbol}&resolution=${resolution}&from=${from}&to=${to}&token=${this.finnhubApiKey}`;
            } else {
                // Stocks
                url = `${this.baseUrl}/stock/candle?symbol=${finnhubSymbol}&resolution=${resolution}&from=${from}&to=${to}&token=${this.finnhubApiKey}`;
            }

            const response = await axios.get(url, { timeout: 10000 });
            
            if (response.data.s !== 'ok' || !response.data.t) {
                return [];
            }

            // Convert Finnhub format to our format
            const candles = [];
            for (let i = 0; i < response.data.t.length; i++) {
                candles.push({
                    timestamp: new Date(response.data.t[i] * 1000),
                    open: response.data.o[i],
                    high: response.data.h[i],
                    low: response.data.l[i],
                    close: response.data.c[i],
                    volume: response.data.v ? response.data.v[i] : 0
                });
            }

            return candles;
        } catch (error) {
            if (error.response?.status === 429) {
                console.log('⏳ Rate limited, waiting...');
                await this.sleep(60000); // Wait 1 minute
            }
            throw error;
        }
    }

    /**
     * Save candles to database
     */
    async saveCandles(symbol, candles) {
        for (const candle of candles) {
            try {
                await database.pool.execute(`
                    INSERT INTO pulse_market_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES (?, 'M1', ?, ?, ?, ?, ?, ?, 0)
                    ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = GREATEST(high, VALUES(high)),
                    low = LEAST(low, VALUES(low)),
                    close = VALUES(close),
                    volume = volume + VALUES(volume)
                `, [
                    symbol,
                    candle.timestamp,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume
                ]);
            } catch (error) {
                // Ignore duplicate errors
                if (!error.code?.includes('ER_DUP_ENTRY')) {
                    console.error('Error saving candle:', error.message);
                }
            }
        }
    }

    /**
     * Schedule periodic gap checks
     */
    startPeriodicCheck(intervalMinutes = 60) {
        console.log(`📅 Scheduling gap checks every ${intervalMinutes} minutes`);
        
        setInterval(async () => {
            console.log('🔍 Running scheduled gap check...');
            await this.recoverOnStartup();
        }, intervalMinutes * 60 * 1000);
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = new GapRecoveryService();