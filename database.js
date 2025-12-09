// =============================================================================
// database.js - Database connection and methods
// =============================================================================

const mysql = require('mysql2/promise');
const crypto = require('crypto');

class Database {
    constructor() {
        this.pool = null;
    }

    async connect() {
        try {
            // Use connection pool for better performance
            this.pool = mysql.createPool({
                host: process.env.DB_HOST || 'localhost',
                user: process.env.DB_USER || 'root',
                password: process.env.DB_PASSWORD || '',
                database: process.env.DB_NAME || 'pulse_markets',
                waitForConnections: true,
                connectionLimit: 10,
                queueLimit: 0,
                enableKeepAlive: true,
                keepAliveInitialDelay: 0
            });

            // Test connection
            const conn = await this.pool.getConnection();
            console.log('✅ Connected to MySQL database');
            conn.release();

            await this.initTables();
        } catch (error) {
            console.error('❌ Database connection failed:', error);
            throw error;
        }
    }

    // Alias for backwards compatibility
    get connection() {
        return this.pool;
    }

    // Check if connected
    isConnected() {
        return this.pool !== null;
    }

    async initTables() {
        // Ensure candle data table exists
        const createCandleTable = `
            CREATE TABLE IF NOT EXISTS pulse_market_data (
                id BIGINT AUTO_INCREMENT,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(10) NOT NULL,
                timestamp DATETIME NOT NULL,
                open DECIMAL(18,8),
                high DECIMAL(18,8),
                low DECIMAL(18,8),
                close DECIMAL(18,8),
                volume DECIMAL(24,8) DEFAULT 0,
                spread DECIMAL(16,2) DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY unique_candle (symbol, timeframe, timestamp),
                INDEX idx_symbol_time (symbol, timestamp),
                INDEX idx_timeframe (timeframe)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        `;

        try {
            await this.pool.execute(createCandleTable);
            console.log('✅ Table pulse_market_data ensured');
            
            // Ensure spread column can handle large values (for existing tables)
            try {
                await this.pool.execute(`
                    ALTER TABLE pulse_market_data 
                    MODIFY COLUMN spread DECIMAL(16,2) DEFAULT 0
                `);
            } catch (alterError) {
                // Ignore if column is already correct or table doesn't exist yet
            }
        } catch (error) {
            console.error('❌ Error creating candle table:', error);
        }
    }

    // =========================================================================
    // API KEY VALIDATION
    // =========================================================================

    /**
     * Validate API key and return user + plan info
     * @param {string} apiKey - The API key (fx_...)
     * @param {string} apiSecret - The API secret (fxs_...) - optional based on your choice
     * @returns {object|null} - User and plan info or null if invalid
     */
    async validateApiKey(apiKey, apiSecret = null) {
        try {
            // Get API key record with user and plan info
            const [rows] = await this.pool.execute(`
                SELECT 
                    ak.id as api_key_id,
                    ak.user_id,
                    ak.name as key_name,
                    ak.key,
                    ak.secret_hash,
                    ak.permissions,
                    ak.allowed_ips,
                    ak.is_active,
                    ak.expires_at,
                    u.name as user_name,
                    u.email,
                    u.is_active as user_active,
                    p.id as plan_id,
                    p.name as plan_name,
                    p.slug as plan_slug,
                    p.api_calls_per_day,
                    p.api_calls_per_minute,
                    p.websocket_access,
                    p.websocket_connections,
                    p.historical_data_access,
                    p.historical_data_days,
                    p.features as plan_features
                FROM api_keys ak
                JOIN users u ON ak.user_id = u.id
                LEFT JOIN subscriptions s ON s.user_id = u.id 
                    AND s.status = 'active' 
                    AND (s.ends_at IS NULL OR s.ends_at > NOW())
                LEFT JOIN plans p ON s.plan_id = p.id
                WHERE ak.key = ?
                    AND ak.is_active = 1
                    AND (ak.expires_at IS NULL OR ak.expires_at > NOW())
                LIMIT 1
            `, [apiKey]);

            if (rows.length === 0) {
                return null;
            }

            const record = rows[0];

            // Check if user is active
            if (!record.user_active) {
                return null;
            }

            // Verify secret if provided (bcrypt comparison)
            if (apiSecret && record.secret_hash) {
                try {
                    const bcrypt = require('bcryptjs');
                    const isValid = await bcrypt.compare(apiSecret, record.secret_hash);
                    if (!isValid) {
                        return null;
                    }
                } catch (e) {
                    console.error('bcryptjs not available or error:', e.message);
                    // If bcrypt fails and secret was required, reject
                    return null;
                }
            }

            // Update last_used_at
            await this.pool.execute(
                'UPDATE api_keys SET last_used_at = NOW() WHERE id = ?',
                [record.api_key_id]
            );

            // Determine tier based on plan
            let tier = 'free';
            if (record.plan_slug === 'individual') tier = 'individual';
            else if (record.plan_slug === 'business') tier = 'business';

            return {
                apiKeyId: record.api_key_id,
                userId: record.user_id,
                userName: record.user_name,
                email: record.email,
                keyName: record.key_name,
                permissions: JSON.parse(record.permissions || '["read"]'),
                allowedIps: record.allowed_ips ? record.allowed_ips.split('\n').filter(Boolean) : [],
                plan: {
                    id: record.plan_id,
                    name: record.plan_name || 'Free',
                    slug: record.plan_slug || 'free',
                    tier: tier,
                    apiCallsPerDay: record.api_calls_per_day || 100,
                    apiCallsPerMinute: record.api_calls_per_minute || 10,
                    websocketAccess: !!record.websocket_access,
                    websocketConnections: record.websocket_connections || 0,
                    historicalDataAccess: !!record.historical_data_access,
                    historicalDataDays: record.historical_data_days || 0,
                    features: JSON.parse(record.plan_features || '{}')
                }
            };
        } catch (error) {
            console.error('❌ Error validating API key:', error);
            return null;
        }
    }

    // =========================================================================
    // USAGE LOGGING
    // =========================================================================

    /**
     * Log an API call
     */
    async logApiCall(userId, apiKeyId, type, details = {}) {
        try {
            await this.pool.execute(`
                INSERT INTO api_usage_logs 
                (user_id, api_key_id, type, endpoint, method, response_code, response_time_ms, ip_address, usage_date, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURDATE(), NOW(), NOW())
            `, [
                userId,
                apiKeyId,
                type,
                details.endpoint || null,
                details.method || null,
                details.responseCode || null,
                details.responseTimeMs || null,
                details.ipAddress || null
            ]);

            // Also update usage_statistics
            await this.incrementUsageStats(userId, type);
        } catch (error) {
            console.error('❌ Error logging API call:', error);
        }
    }

    /**
     * Increment usage statistics
     */
    async incrementUsageStats(userId, type) {
        const field = type === 'rest' ? 'rest_api_calls' : 
                      type === 'websocket_connect' ? 'websocket_connections' :
                      type === 'websocket_message' ? 'websocket_messages' :
                      type === 'historical' ? 'historical_requests' : 'rest_api_calls';

        try {
            await this.pool.execute(`
                INSERT INTO usage_statistics (user_id, date, ${field}, created_at, updated_at)
                VALUES (?, CURDATE(), 1, NOW(), NOW())
                ON DUPLICATE KEY UPDATE 
                    ${field} = ${field} + 1,
                    updated_at = NOW()
            `, [userId]);
        } catch (error) {
            console.error('❌ Error incrementing usage stats:', error);
        }
    }

    /**
     * Get today's usage for a user
     */
    async getTodayUsage(userId) {
        try {
            const [rows] = await this.pool.execute(`
                SELECT rest_api_calls, websocket_connections, websocket_messages, historical_requests
                FROM usage_statistics
                WHERE user_id = ? AND date = CURDATE()
            `, [userId]);

            if (rows.length === 0) {
                return { restApiCalls: 0, websocketConnections: 0, websocketMessages: 0, historicalRequests: 0 };
            }

            return {
                restApiCalls: rows[0].rest_api_calls || 0,
                websocketConnections: rows[0].websocket_connections || 0,
                websocketMessages: rows[0].websocket_messages || 0,
                historicalRequests: rows[0].historical_requests || 0
            };
        } catch (error) {
            console.error('❌ Error getting today usage:', error);
            return { restApiCalls: 0, websocketConnections: 0, websocketMessages: 0, historicalRequests: 0 };
        }
    }

    // =========================================================================
    // QUOTE DATA
    // =========================================================================

    /**
     * Get latest price for a symbol
     */
    async getLatestQuote(symbol) {
        try {
            const [rows] = await this.pool.execute(`
                SELECT timestamp, open, high, low, close, volume, spread
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = 'M1'
                ORDER BY timestamp DESC
                LIMIT 1
            `, [symbol]);

            if (rows.length === 0) return null;

            const row = rows[0];
            return {
                symbol: symbol,
                bid: parseFloat(row.close),
                ask: parseFloat(row.close) + parseFloat(row.spread || 0),
                mid: parseFloat(row.close) + (parseFloat(row.spread || 0) / 2),
                spread: parseFloat(row.spread || 0),
                high: parseFloat(row.high),
                low: parseFloat(row.low),
                open: parseFloat(row.open),
                close: parseFloat(row.close),
                volume: parseFloat(row.volume || 0),
                timestamp: row.timestamp
            };
        } catch (error) {
            console.error('❌ Error getting latest quote:', error);
            return null;
        }
    }

    /**
     * Get latest quotes for multiple symbols
     */
    async getLatestQuotes(symbols) {
        if (!symbols || symbols.length === 0) return [];

        try {
            const placeholders = symbols.map(() => '?').join(',');
            const [rows] = await this.pool.execute(`
                SELECT p1.symbol, p1.timestamp, p1.open, p1.high, p1.low, p1.close, p1.volume, p1.spread
                FROM pulse_market_data p1
                INNER JOIN (
                    SELECT symbol, MAX(timestamp) as max_ts
                    FROM pulse_market_data
                    WHERE symbol IN (${placeholders}) AND timeframe = 'M1'
                    GROUP BY symbol
                ) p2 ON p1.symbol = p2.symbol AND p1.timestamp = p2.max_ts AND p1.timeframe = 'M1'
            `, symbols);

            return rows.map(row => ({
                symbol: row.symbol,
                bid: parseFloat(row.close),
                ask: parseFloat(row.close) + parseFloat(row.spread || 0),
                spread: parseFloat(row.spread || 0),
                timestamp: row.timestamp
            }));
        } catch (error) {
            console.error('❌ Error getting latest quotes:', error);
            return [];
        }
    }

    /**
     * Get daily change data
     */
    async getDailyChange(symbol) {
        try {
            const [rows] = await this.pool.execute(`
                SELECT open, high, low, close
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = 'D1'
                ORDER BY timestamp DESC
                LIMIT 2
            `, [symbol]);

            if (rows.length < 2) return { change: 0, changePercent: 0, high: 0, low: 0 };

            const today = rows[0];
            const yesterday = rows[1];

            const change = parseFloat(today.close) - parseFloat(yesterday.close);
            const changePercent = (change / parseFloat(yesterday.close)) * 100;

            return {
                change: change,
                changePercent: changePercent,
                high: parseFloat(today.high),
                low: parseFloat(today.low)
            };
        } catch (error) {
            console.error('❌ Error getting daily change:', error);
            return { change: 0, changePercent: 0, high: 0, low: 0 };
        }
    }

    // =========================================================================
    // HISTORICAL DATA
    // =========================================================================

    /**
     * Get historical candles
     */
    async getCandles(symbol, timeframe, options = {}) {
        const { from, to, limit = 500 } = options;

        try {
            let query = `
                SELECT timestamp, open, high, low, close, volume
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `;
            const params = [symbol, timeframe];

            if (from) {
                query += ' AND timestamp >= ?';
                params.push(from);
            }
            if (to) {
                query += ' AND timestamp <= ?';
                params.push(to);
            }

            query += ' ORDER BY timestamp DESC LIMIT ?';
            params.push(Math.min(limit, 5000));

            const [rows] = await this.pool.execute(query, params);

            return rows.reverse().map(row => ({
                timestamp: row.timestamp,
                open: parseFloat(row.open),
                high: parseFloat(row.high),
                low: parseFloat(row.low),
                close: parseFloat(row.close),
                volume: parseFloat(row.volume || 0)
            }));
        } catch (error) {
            console.error('❌ Error getting candles:', error);
            return [];
        }
    }

    /**
     * Get the latest candle timestamp for gap detection
     */
    async getLatestCandleTime(symbol, timeframe) {
        try {
            const [rows] = await this.pool.execute(`
                SELECT MAX(timestamp) as latest
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `, [symbol, timeframe]);

            return rows[0]?.latest || null;
        } catch (error) {
            console.error('❌ Error getting latest candle time:', error);
            return null;
        }
    }

    // =========================================================================
    // PLANS
    // =========================================================================

    /**
     * Get all active plans
     */
    async getPlans() {
        try {
            const [rows] = await this.pool.execute(`
                SELECT * FROM plans WHERE is_active = 1 ORDER BY sort_order
            `);
            return rows;
        } catch (error) {
            console.error('❌ Error getting plans:', error);
            return [];
        }
    }

    /**
     * Get plan by slug
     */
    async getPlanBySlug(slug) {
        try {
            const [rows] = await this.pool.execute(
                'SELECT * FROM plans WHERE slug = ? AND is_active = 1',
                [slug]
            );
            return rows[0] || null;
        } catch (error) {
            console.error('❌ Error getting plan:', error);
            return null;
        }
    }

    // =========================================================================
    // CLEANUP
    // =========================================================================

    async cleanupOldCandles() {
        try {
            // Keep last 200,000 candles per symbol/timeframe
            // This is a simplified version - adjust based on your needs
            const [result] = await this.pool.execute(`
                DELETE FROM pulse_market_data 
                WHERE timestamp < DATE_SUB(NOW(), INTERVAL 90 DAY)
                AND timeframe = 'M1'
            `);
            console.log(`🧹 Cleaned up ${result.affectedRows} old M1 candles`);
        } catch (error) {
            console.error('❌ Error cleaning up old candles:', error);
        }
    }

    async disconnect() {
        if (this.pool) {
            await this.pool.end();
            console.log('📌 Database connection pool closed');
        }
    }
}

module.exports = new Database();