// =============================================================================
// middleware/auth.js - API Key Authentication
// =============================================================================

const database = require('../database');

// Configuration: Set to true to require both key and secret
const REQUIRE_SECRET = false; // Set to true for key + secret validation

/**
 * API Authentication Middleware
 * Validates API key (and optionally secret) from headers
 */
async function authenticate(req, res, next) {
    const startTime = Date.now();
    
    // Get API key from header
    const apiKey = req.header('X-API-Key');
    const apiSecret = req.header('X-API-Secret');

    // Check if API key is provided
    if (!apiKey) {
        return res.status(401).json({
            success: false,
            error: {
                code: 'MISSING_API_KEY',
                message: 'X-API-Key header is required',
                status: 401
            }
        });
    }

    // Validate API key format
    if (!apiKey.startsWith('fx_')) {
        return res.status(401).json({
            success: false,
            error: {
                code: 'INVALID_API_KEY',
                message: 'Invalid API key format',
                status: 401
            }
        });
    }

    // Check if secret is required but not provided
    if (REQUIRE_SECRET && !apiSecret) {
        return res.status(401).json({
            success: false,
            error: {
                code: 'MISSING_API_SECRET',
                message: 'X-API-Secret header is required',
                status: 401
            }
        });
    }

    try {
        // Validate against database
        const authResult = await database.validateApiKey(
            apiKey, 
            REQUIRE_SECRET ? apiSecret : null
        );

        if (!authResult) {
            return res.status(401).json({
                success: false,
                error: {
                    code: 'INVALID_CREDENTIALS',
                    message: 'Invalid API key or secret',
                    status: 401
                }
            });
        }

        // Check IP whitelist if configured
        if (authResult.allowedIps && authResult.allowedIps.length > 0) {
            const clientIp = getClientIp(req);
            if (!isIpAllowed(clientIp, authResult.allowedIps)) {
                return res.status(403).json({
                    success: false,
                    error: {
                        code: 'IP_NOT_ALLOWED',
                        message: 'Request IP is not whitelisted',
                        status: 403
                    }
                });
            }
        }

        // Attach auth info to request
        req.auth = authResult;
        req.authTime = Date.now() - startTime;

        next();
    } catch (error) {
        console.error('❌ Authentication error:', error);
        return res.status(500).json({
            success: false,
            error: {
                code: 'INTERNAL_ERROR',
                message: 'Authentication failed',
                status: 500
            }
        });
    }
}

/**
 * Optional authentication - doesn't fail if no key provided
 * Useful for endpoints that have different behavior for auth/unauth users
 */
async function optionalAuth(req, res, next) {
    const apiKey = req.header('X-API-Key');
    
    if (!apiKey) {
        req.auth = null;
        return next();
    }

    // If key is provided, validate it
    return authenticate(req, res, next);
}

/**
 * Check if a feature is available for the user's plan
 */
function requireFeature(feature) {
    return (req, res, next) => {
        if (!req.auth) {
            return res.status(401).json({
                success: false,
                error: {
                    code: 'UNAUTHORIZED',
                    message: 'Authentication required',
                    status: 401
                }
            });
        }

        const plan = req.auth.plan;
        let hasFeature = false;

        switch (feature) {
            case 'websocket':
                hasFeature = plan.websocketAccess;
                break;
            case 'historical':
                hasFeature = plan.historicalDataAccess;
                break;
            case 'tick_data':
                hasFeature = plan.features?.tick_data || false;
                break;
            default:
                hasFeature = true;
        }

        if (!hasFeature) {
            const featureLabels = {
                websocket: 'WebSocket access',
                historical: 'Historical data access',
                tick_data: 'Tick-by-tick data'
            };

            return res.status(403).json({
                success: false,
                error: {
                    code: 'PLAN_LIMIT_EXCEEDED',
                    message: `${featureLabels[feature] || feature} requires a paid plan`,
                    status: 403
                }
            });
        }

        next();
    };
}

/**
 * Get client IP address
 */
function getClientIp(req) {
    return req.headers['x-forwarded-for']?.split(',')[0]?.trim() ||
           req.headers['x-real-ip'] ||
           req.connection?.remoteAddress ||
           req.socket?.remoteAddress ||
           req.ip;
}

/**
 * Check if IP is in allowed list
 */
function isIpAllowed(ip, allowedIps) {
    if (!allowedIps || allowedIps.length === 0) return true;

    for (const allowed of allowedIps) {
        if (ip === allowed) return true;
        
        // Basic CIDR support
        if (allowed.includes('/') && ipInCidr(ip, allowed)) {
            return true;
        }
    }

    return false;
}

/**
 * Check if IP is in CIDR range
 */
function ipInCidr(ip, cidr) {
    try {
        const [subnet, bits] = cidr.split('/');
        const ipLong = ipToLong(ip);
        const subnetLong = ipToLong(subnet);
        const mask = -1 << (32 - parseInt(bits));
        return (ipLong & mask) === (subnetLong & mask);
    } catch {
        return false;
    }
}

function ipToLong(ip) {
    const parts = ip.split('.');
    return ((parseInt(parts[0]) << 24) +
            (parseInt(parts[1]) << 16) +
            (parseInt(parts[2]) << 8) +
            parseInt(parts[3])) >>> 0;
}

module.exports = {
    authenticate,
    optionalAuth,
    requireFeature,
    getClientIp,
    REQUIRE_SECRET
};