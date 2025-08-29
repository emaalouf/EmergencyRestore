class DatabaseError extends Error {
    constructor(message, table = null, operation = null) {
        super(message);
        this.name = 'DatabaseError';
        this.table = table;
        this.operation = operation;
    }
}

async function executeWithRetry(operation, maxRetries = 3, delay = 1000) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            if (attempt === maxRetries) {
                throw error;
            }
            console.log(`⚠️  Attempt ${attempt} failed, retrying in ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            delay *= 2;
        }
    }
}

function sanitizeTableName(tableName) {
    if (!tableName || typeof tableName !== 'string') {
        throw new Error('Table name must be a non-empty string');
    }
    
    const sanitized = tableName.replace(/[^\w]/g, '');
    if (sanitized.length === 0) {
        throw new Error('Table name contains no valid characters');
    }
    
    return sanitized;
}

function formatProgress(current, total, percentage) {
    return `${current.toLocaleString()}/${total.toLocaleString()} (${percentage.toFixed(1)}%)`;
}

function calculateETA(processed, total, startTime) {
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = processed / elapsed;
    const remaining = total - processed;
    const estimatedSeconds = remaining / rate;
    return Math.ceil(estimatedSeconds / 60);
}

module.exports = {
    DatabaseError,
    executeWithRetry,
    sanitizeTableName,
    formatProgress,
    calculateETA
};