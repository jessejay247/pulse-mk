const mysql = require('mysql2/promise');

async function testConnection() {
    try {
        // Try multiple connection configurations
        const configs = [
            { host: 'localhost', user: 'root', database: 'pulse_market_data' },
            { host: 'localhost', user: 'root', password: null, database: 'pulse_market_data' },
            { host: 'localhost', user: 'root', password: '', database: 'pulse_market_data' },
            { host: '127.0.0.1', user: 'root', database: 'pulse_market_data' }
        ];

        for (let config of configs) {
            console.log(`Trying config:`, config);
            try {
                const connection = await mysql.createConnection(config);
                console.log('✅ SUCCESS with config:', config);
                await connection.end();
                return config;
            } catch (error) {
                console.log('❌ FAILED with config:', config, error.message);
            }
        }
        
        console.log('❌ All connection attempts failed');
    } catch (error) {
        console.error('Test failed:', error);
    }
}

testConnection();