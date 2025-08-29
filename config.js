require('dotenv').config();

function createDatabaseConfig(prefix) {
    return {
        server: process.env[`${prefix}_DB_SERVER`],
        database: process.env[`${prefix}_DB_NAME`],
        user: process.env[`${prefix}_DB_USER`],
        password: process.env[`${prefix}_DB_PASSWORD`],
        options: {
            encrypt: true,
            trustServerCertificate: true,
            enableArithAbort: true
        },
        pool: {
            min: parseInt(process.env.DB_POOL_MIN) || 0,
            max: parseInt(process.env.DB_POOL_MAX) || 10
        },
        requestTimeout: parseInt(process.env.DB_REQUEST_TIMEOUT) || 30000,
        connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 30000
    };
}

function validateConfig(config, name) {
    const required = ['server', 'database', 'user', 'password'];
    const missing = required.filter(field => !config[field]);
    
    if (missing.length > 0) {
        throw new Error(`Missing required ${name} configuration: ${missing.join(', ')}`);
    }
}

const sourceConfig = createDatabaseConfig('SOURCE');
const targetConfig = createDatabaseConfig('TARGET');

validateConfig(sourceConfig, 'source');
validateConfig(targetConfig, 'target');

module.exports = {
    sourceConfig,
    targetConfig
};