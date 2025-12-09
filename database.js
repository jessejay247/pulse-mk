// =============================================================================
// database.js - Database connection and methods (Fixed)
// =============================================================================

const mysql = require('mysql2/promise');

class Database {
    constructor() {
        this.pool = null;
    }

    async connect() {
        try {
            this.pool = mysql.createPool({
                host: process.env.DB_HOST || '195.35.53.3',
                user: process.env.DB_USER || 'u753127729_pulse',
                password: process.env.DB_PASSWORD || 'Hxtx=M52da>3',
                database: process.env.DB_NAME || 'u753127729_pulse',
                waitForConnections: true,
                connectionLimit: 5,  // Reduced for free tier
                queueLimit: 0,
                enableKeepAlive: true,
                keepAliveInitialDelay: 10000,
                connectTimeout: 10000,  // 10 second timeout
                acquireTimeout: 10000,
            });

            // Test connection
            const conn = await this.pool.getConnection();
            console.log('✅ Connected to MySQL database');
            conn.release();

            await this.initTables();
        } catch (error) {
            console.error('❌ Database connection failed:', error.message);
            throw error;
        }
    }

    get connection() {
        return this.pool;
    }

    isConnected() {
        return this.pool !== null;
    }

    async initTables() {
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
        } catch (error) {
            console.error('❌ Error creating candle table:', error.message);
        }
    }

    // =========================================================================
    // API KEY VALIDATION - OPTIMIZED
    // =========================================================================

    /**
     * Validate API key - Split into smaller queries to avoid timeout
     */
    async validateApiKey(apiKey, apiSecret = null) {
        if (!this.pool) {
            console.error('❌ Database pool not initialized');
            return null;
        }

        let conn;
        try {
            // Get a dedicated connection for this transaction
            conn = await this.pool.getConnection();
            
            // Step 1: Get API key record (simple query)
            const [keyRows] = await conn.execute(`
                SELECT id, user_id, name, \`key\`, secret_hash, permissions, allowed_ips, is_active, expires_at
                FROM api_keys 
                WHERE \`key\` = ? AND is_active = 1
                LIMIT 1
            `, [apiKey]);

            if (keyRows.length === 0) {
                console.log('❌ API key not found in database');
                conn.release();
                return null;
            }

            const keyRecord = keyRows[0];

            // Check expiry
            if (keyRecord.expires_at && new Date(keyRecord.expires_at) < new Date()) {
                console.log('❌ API key expired');
                conn.release();
                return null;
            }

            // Step 2: Get user info (simple query)
            const [userRows] = await conn.execute(`
                SELECT id, name, email, is_active FROM users WHERE id = ? LIMIT 1
            `, [keyRecord.user_id]);

            if (userRows.length === 0 || !userRows[0].is_active) {
                console.log('❌ User not found or inactive');
                conn.release();
                return null;
            }

            const user = userRows[0];

            // Step 3: Get subscription and plan (separate query)
            let plan = {
                id: null,
                name: 'Free',
                slug: 'free',
                tier: 'free',
                apiCallsPerDay: 100,
                apiCallsPerMinute: 10,
                websocketAccess: true,  // Allow websocket for testing
                websocketConnections: 1,
                historicalDataAccess: false,
                historicalDataDays: 0,
                features: {}
            };

            try {
                const [subRows] = await conn.execute(`
                    SELECT p.id, p.name, p.slug, p.api_calls_per_day, p.api_calls_per_minute,
                           p.websocket_access, p.websocket_connections, 
                           p.historical_data_access, p.historical_data_days, p.features
                    FROM subscriptions s
                    JOIN plans p ON s.plan_id = p.id
                    WHERE s.user_id = ? AND s.status = 'active'
                    AND (s.ends_at IS NULL OR s.ends_at > NOW())
                    ORDER BY p.sort_order DESC
                    LIMIT 1
                `, [user.id]);

                if (subRows.length > 0) {
                    const p = subRows[0];
                    let tier = 'free';
                    if (p.slug === 'individual') tier = 'individual';
                    else if (p.slug === 'business') tier = 'business';

                    plan = {
                        id: p.id,
                        name: p.name,
                        slug: p.slug,
                        tier: tier,
                        apiCallsPerDay: p.api_calls_per_day || 100,
                        apiCallsPerMinute: p.api_calls_per_minute || 10,
                        websocketAccess: !!p.websocket_access,
                        websocketConnections: p.websocket_connections || 1,
                        historicalDataAccess: !!p.historical_data_access,
                        historicalDataDays: p.historical_data_days || 0,
                        features: p.features ? JSON.parse(p.features) : {}
                    };
                }
            } catch (planError) {
                console.log('⚠️ Could not fetch plan, using defaults:', planError.message);
                // Continue with default free plan
            }

            // Step 4: Update last_used_at (fire and forget)
            conn.execute(
                'UPDATE api_keys SET last_used_at = NOW() WHERE id = ?',
                [keyRecord.id]
            ).catch(() => {}); // Ignore errors

            conn.release();

            // Parse permissions safely
            let permissions = ['read'];
            try {
                permissions = JSON.parse(keyRecord.permissions || '["read"]');
            } catch {}

            // Parse allowed IPs
            let allowedIps = [];
            if (keyRecord.allowed_ips) {
                allowedIps = keyRecord.allowed_ips.split('\n').filter(Boolean);
            }

            console.log('✅ API key validated for user:', user.name, 'plan:', plan.name);

            return {
                apiKeyId: keyRecord.id,
                userId: user.id,
                userName: user.name,
                email: user.email,
                keyName: keyRecord.name,
                permissions,
                allowedIps,
                plan
            };

        } catch (error) {
            console.error('❌ Error validating API key:', error.message);
            if (conn) conn.release();
            return null;
        }
    }

    // =========================================================================
    // USAGE LOGGING
    // =========================================================================

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

            await this.incrementUsageStats(userId, type);
        } catch (error) {
            // Silent fail for logging
        }
    }

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
            // Silent fail
        }
    }

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
            return { restApiCalls: 0, websocketConnections: 0, websocketMessages: 0, historicalRequests: 0 };
        }
    }

    // =========================================================================
    // QUOTE DATA
    // =========================================================================

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
            console.error('❌ Error getting latest quote:', error.message);
            return null;
        }
    }

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
            return [];
        }
    }

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
            return { change: 0, changePercent: 0, high: 0, low: 0 };
        }
    }

    // =========================================================================
    // HISTORICAL DATA
    // =========================================================================

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
            console.error('❌ Error getting candles:', error.message);
            return [];
        }
    }

    async getLatestCandleTime(symbol, timeframe) {
        try {
            const [rows] = await this.pool.execute(`
                SELECT MAX(timestamp) as latest
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `, [symbol, timeframe]);

            return rows[0]?.latest || null;
        } catch (error) {
            return null;
        }
    }

    // =========================================================================
    // PLANS
    // =========================================================================

    async getPlans() {
        try {
            const [rows] = await this.pool.execute(`
                SELECT * FROM plans WHERE is_active = 1 ORDER BY sort_order
            `);
            return rows;
        } catch (error) {
            return [];
        }
    }

    async getPlanBySlug(slug) {
        try {
            const [rows] = await this.pool.execute(
                'SELECT * FROM plans WHERE slug = ? AND is_active = 1',
                [slug]
            );
            return rows[0] || null;
        } catch (error) {
            return null;
        }
    }

    // =========================================================================
    // CLEANUP
    // =========================================================================

    async cleanupOldCandles() {
        try {
            const [result] = await this.pool.execute(`
                DELETE FROM pulse_market_data 
                WHERE timestamp < DATE_SUB(NOW(), INTERVAL 90 DAY)
                AND timeframe = 'M1'
            `);
            console.log(`🧹 Cleaned up ${result.affectedRows} old M1 candles`);
        } catch (error) {
            console.error('❌ Error cleaning up old candles:', error.message);
        }
    }

    async disconnect() {
        if (this.pool) {
            await this.pool.end();
            console.log('🔌 Database connection pool closed');
        }
    }
}

module.exports = new Database();