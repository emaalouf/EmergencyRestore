const sql = require('mssql');
const { sourceConfig, targetConfig } = require('./config');
const { getTableList, getTableSchema } = require('./database');

async function fixAppBinaryObjectsStructure(sourcePool, targetPool) {
    console.log('🔧 Fixing AppBinaryObjects structure...');
    
    try {
        console.log('🔄 Recreating table with correct structure...');
        await targetPool.request().query(`DROP TABLE IF EXISTS [AppBinaryObjects]`);
        
        await targetPool.request().query(`
            CREATE TABLE [AppBinaryObjects] (
                [Id] UNIQUEIDENTIFIER NOT NULL,
                [Bytes] VARBINARY(MAX),
                [TenantId] INT,
                PRIMARY KEY ([Id])
            )
        `);
        console.log('✅ Recreated AppBinaryObjects table with correct structure');
    } catch (error) {
        console.error('❌ Failed to recreate AppBinaryObjects table:', error.message);
        throw error;
    }
}

async function fixStructureIssues() {
    let sourcePool, targetPool;
    
    try {
        console.log('🔌 Connecting to databases...');
        sourcePool = await sql.connect(sourceConfig);
        targetPool = new sql.ConnectionPool(targetConfig);
        await targetPool.connect();
        console.log('✅ Connected to both databases');
        
        await fixAppBinaryObjectsStructure(sourcePool, targetPool);
        
        console.log('🎉 Structure fixes completed!');
        
    } catch (error) {
        console.error('💥 Structure fix failed:', error.message);
        throw error;
    } finally {
        if (sourcePool) {
            await sourcePool.close();
        }
        if (targetPool) {
            await targetPool.close();
        }
    }
}

if (require.main === module) {
    fixStructureIssues()
        .then(() => {
            console.log('✅ Run fixer.js again to complete data fixes');
            process.exit(0);
        })
        .catch(error => {
            console.error('Structure fixer failed:', error.message);
            process.exit(1);
        });
}

module.exports = { fixStructureIssues, fixAppBinaryObjectsStructure };