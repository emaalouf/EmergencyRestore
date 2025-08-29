const sql = require('mssql');
const { sourceConfig, targetConfig } = require('./config');
const { transferData, transferFunctions, transferViews } = require('./transfer');
const { clearTargetDatabase } = require('./database');

async function restoreDatabase() {
    let sourcePool, targetPool;
    
    try {
        console.log('🔌 Connecting to source database...');
        sourcePool = await sql.connect(sourceConfig);
        console.log('✅ Connected to source database');
        
        console.log('🔌 Connecting to target database...');
        targetPool = new sql.ConnectionPool(targetConfig);
        await targetPool.connect();
        console.log('✅ Connected to target database');
        
        await clearTargetDatabase(targetPool);
        
        await transferData(sourcePool, targetPool);
        
        await transferFunctions(sourcePool, targetPool);
        
        await transferViews(sourcePool, targetPool);
        
        console.log('🎉 Database restore completed successfully!');
        
    } catch (error) {
        console.error('💥 Database restore failed:', error.message);
        process.exit(1);
    } finally {
        if (sourcePool) {
            await sourcePool.close();
            console.log('🔌 Source database connection closed');
        }
        if (targetPool) {
            await targetPool.close();
            console.log('🔌 Target database connection closed');
        }
    }
}

if (require.main === module) {
    restoreDatabase();
}

module.exports = { restoreDatabase };