const { executeWithRetry, formatProgress, calculateETA } = require('./utils');
const { getTableList, getTableSchema, createTableIfNotExists, bulkInsertData, getFunctions, getViews } = require('./database');

async function transferTableData(sourcePool, targetPool, table) {
    const tableStartTime = Date.now();
    
    try {
        const schema = await getTableSchema(sourcePool, table);
        await createTableIfNotExists(targetPool, table, schema);
        
        const countResult = await sourcePool.request().query(`SELECT COUNT(*) as total FROM [${table}]`);
        const totalRows = countResult.recordset[0].total;
        
        if (totalRows === 0) {
            console.log(`ℹ️  Table ${table} is empty (0 rows), skipping...`);
            return { transferred: 0, total: 0 };
        }
        
        console.log(`📊 Table ${table}: ${totalRows.toLocaleString()} rows to transfer`);
        
        const BATCH_SIZE = 10000;
        let offset = 0;
        let insertedCount = 0;
        
        while (offset < totalRows) {
            const batchStartTime = Date.now();
            
            const sourceData = await sourcePool.request().query(`
                SELECT * FROM [${table}]
                ORDER BY (SELECT NULL)
                OFFSET ${offset} ROWS
                FETCH NEXT ${BATCH_SIZE} ROWS ONLY
            `);
            
            if (sourceData.recordset.length > 0) {
                try {
                    await executeWithRetry(() => 
                        bulkInsertData(targetPool, table, schema, sourceData.recordset)
                    );
                    
                    insertedCount += sourceData.recordset.length;
                    
                    const batchTime = (Date.now() - batchStartTime) / 1000;
                    const progress = (insertedCount / totalRows) * 100;
                    const avgRowsPerSecond = insertedCount / ((Date.now() - tableStartTime) / 1000);
                    const estimatedMinutesLeft = calculateETA(insertedCount, totalRows, tableStartTime);
                    
                    console.log(`   🚀 ${formatProgress(insertedCount, totalRows, progress)} | Speed: ${Math.round(avgRowsPerSecond).toLocaleString()} rows/sec | ETA: ${estimatedMinutesLeft}min`);
                } catch (error) {
                    console.error(`❌ Error bulk inserting batch for table ${table}:`, error.message);
                    break;
                }
            }
            
            offset += BATCH_SIZE;
        }
        
        const tableTime = (Date.now() - tableStartTime) / 1000;
        console.log(`✅ Table ${table}: ${formatProgress(insertedCount, totalRows, 100)} transferred in ${tableTime.toFixed(1)}s`);
        
        return { transferred: insertedCount, total: totalRows };
        
    } catch (error) {
        console.error(`❌ Error transferring table ${table}:`, error.message);
        throw error;
    }
}

async function transferData(sourcePool, targetPool) {
    console.log('📊 Starting data transfer...');
    
    const tables = await getTableList(sourcePool);
    const totalTables = tables.length;
    let completedTables = 0;
    const overallStartTime = Date.now();
    
    for (const table of tables) {
        try {
            console.log(`🔄 [${completedTables + 1}/${totalTables}] Transferring table: ${table}`);
            await transferTableData(sourcePool, targetPool, table);
        } catch (error) {
            console.error(`❌ Error transferring table ${table}:`, error.message);
        }
        
        completedTables++;
        const overallProgress = (completedTables / totalTables) * 100;
        const estimatedOverallMinutes = calculateETA(completedTables, totalTables, overallStartTime);
        
        console.log(`📈 Overall Progress: ${formatProgress(completedTables, totalTables, overallProgress)} | ETA: ${estimatedOverallMinutes}min\n`);
    }
    
    const totalTime = (Date.now() - overallStartTime) / 1000;
    console.log(`✅ Data transfer completed in ${(totalTime / 60).toFixed(1)} minutes`);
}

async function transferFunctions(sourcePool, targetPool) {
    console.log('🔧 Transferring functions...');
    
    try {
        const functions = await getFunctions(sourcePool);
        
        for (const func of functions) {
            try {
                await executeWithRetry(async () => {
                    await targetPool.request().query(`DROP FUNCTION IF EXISTS [${func.ROUTINE_NAME}]`);
                    await targetPool.request().query(func.ROUTINE_DEFINITION);
                });
                console.log(`✅ Transferred function: ${func.ROUTINE_NAME}`);
            } catch (error) {
                console.error(`❌ Error transferring function ${func.ROUTINE_NAME}:`, error.message);
            }
        }
        
        console.log(`✅ Functions transfer completed (${functions.length} functions)`);
    } catch (error) {
        console.error('❌ Error getting functions:', error.message);
    }
}

async function transferViews(sourcePool, targetPool) {
    console.log('👁️ Transferring views...');
    
    try {
        const views = await getViews(sourcePool);
        
        for (const view of views) {
            try {
                await executeWithRetry(async () => {
                    await targetPool.request().query(`DROP VIEW IF EXISTS [${view.VIEW_NAME}]`);
                    await targetPool.request().query(view.VIEW_DEFINITION);
                });
                console.log(`✅ Transferred view: ${view.VIEW_NAME}`);
            } catch (error) {
                console.error(`❌ Error transferring view ${view.VIEW_NAME}:`, error.message);
            }
        }
        
        console.log(`✅ Views transfer completed (${views.length} views)`);
    } catch (error) {
        console.error('❌ Error getting views:', error.message);
    }
}

module.exports = {
    transferData,
    transferFunctions,
    transferViews,
    transferTableData
};