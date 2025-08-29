const sql = require('mssql');
const { sourceConfig, targetConfig } = require('./config');
const { getTableList, getTableSchema, bulkInsertData } = require('./database');
const { executeWithRetry, formatProgress, calculateETA } = require('./utils');
const { verifyTableData, compareTableStructure } = require('./verifier');

async function fixTableStructure(targetPool, tableName, sourceMissing, targetExtra) {
    console.log(`🔧 Fixing table structure: ${tableName}`);
    
    for (const column of sourceMissing) {
        try {
            let columnDef = `[${column.COLUMN_NAME}] ${column.DATA_TYPE}`;
            
            if (column.CHARACTER_MAXIMUM_LENGTH && column.CHARACTER_MAXIMUM_LENGTH > 0) {
                columnDef += `(${column.CHARACTER_MAXIMUM_LENGTH})`;
            } else if (column.DATA_TYPE === 'decimal' || column.DATA_TYPE === 'numeric') {
                if (column.NUMERIC_PRECISION && column.NUMERIC_SCALE !== null) {
                    columnDef += `(${column.NUMERIC_PRECISION},${column.NUMERIC_SCALE})`;
                }
            } else if (column.DATA_TYPE === 'varchar' || column.DATA_TYPE === 'nvarchar') {
                if (column.CHARACTER_MAXIMUM_LENGTH === -1) {
                    columnDef += '(MAX)';
                } else if (!column.CHARACTER_MAXIMUM_LENGTH) {
                    columnDef += '(255)';
                }
            }
            
            if (column.IS_NULLABLE === 'NO') {
                columnDef += ' NOT NULL';
            }
            
            await targetPool.request().query(`ALTER TABLE [${tableName}] ADD ${columnDef}`);
            console.log(`   ✅ Added column: ${column.COLUMN_NAME}`);
        } catch (error) {
            console.error(`   ❌ Failed to add column ${column.COLUMN_NAME}:`, error.message);
        }
    }
    
    for (const column of targetExtra) {
        try {
            await targetPool.request().query(`ALTER TABLE [${tableName}] DROP COLUMN [${column.COLUMN_NAME}]`);
            console.log(`   ✅ Removed extra column: ${column.COLUMN_NAME}`);
        } catch (error) {
            console.error(`   ❌ Failed to remove column ${column.COLUMN_NAME}:`, error.message);
        }
    }
}

async function retransferTableData(sourcePool, targetPool, tableName) {
    console.log(`🔄 Re-transferring data for: ${tableName}`);
    
    try {
        await targetPool.request().query(`DELETE FROM [${tableName}]`);
        console.log(`   🧹 Cleared target table: ${tableName}`);
        
        const schema = await getTableSchema(sourcePool, tableName);
        const countResult = await sourcePool.request().query(`SELECT COUNT(*) as total FROM [${tableName}]`);
        const totalRows = countResult.recordset[0].total;
        
        if (totalRows === 0) {
            console.log(`   ℹ️  Table ${tableName} is empty, skipping data transfer`);
            return;
        }
        
        console.log(`   📊 Transferring ${totalRows.toLocaleString()} rows`);
        
        const BATCH_SIZE = 5000;
        let offset = 0;
        let insertedCount = 0;
        const startTime = Date.now();
        
        while (offset < totalRows) {
            try {
                const sourceData = await executeWithRetry(async () => {
                    return await sourcePool.request().query(`
                        SELECT * FROM [${tableName}]
                        ORDER BY (SELECT NULL)
                        OFFSET ${offset} ROWS
                        FETCH NEXT ${BATCH_SIZE} ROWS ONLY
                    `);
                });
                
                if (sourceData.recordset.length > 0) {
                    await executeWithRetry(async () => {
                        await bulkInsertData(targetPool, tableName, schema, sourceData.recordset);
                    });
                    
                    insertedCount += sourceData.recordset.length;
                    const progress = (insertedCount / totalRows) * 100;
                    const avgRowsPerSecond = insertedCount / ((Date.now() - startTime) / 1000);
                    const etaMinutes = calculateETA(insertedCount, totalRows, startTime);
                    
                    if (insertedCount % 25000 === 0 || insertedCount === totalRows) {
                        console.log(`   🚀 ${formatProgress(insertedCount, totalRows, progress)} | Speed: ${Math.round(avgRowsPerSecond).toLocaleString()} rows/sec | ETA: ${etaMinutes}min`);
                    }
                }
                
                offset += BATCH_SIZE;
                
            } catch (error) {
                console.error(`   ❌ Error transferring batch at offset ${offset}:`, error.message);
                
                if (error.message.includes('OLE DB') || error.message.includes('invalid data')) {
                    console.log(`   🔧 Trying individual row insertion for problematic batch...`);
                    await transferBatchIndividually(sourcePool, targetPool, tableName, schema, offset, Math.min(BATCH_SIZE, totalRows - offset));
                    insertedCount += Math.min(BATCH_SIZE, totalRows - offset);
                    offset += BATCH_SIZE;
                } else {
                    throw error;
                }
            }
        }
        
        const transferTime = (Date.now() - startTime) / 1000;
        console.log(`   ✅ Re-transfer completed: ${insertedCount.toLocaleString()} rows in ${transferTime.toFixed(1)}s`);
        
    } catch (error) {
        console.error(`   ❌ Failed to re-transfer table ${tableName}:`, error.message);
        throw error;
    }
}

async function transferBatchIndividually(sourcePool, targetPool, tableName, schema, offset, batchSize) {
    console.log(`   🐌 Transferring ${batchSize} rows individually...`);
    
    const sourceData = await sourcePool.request().query(`
        SELECT * FROM [${tableName}]
        ORDER BY (SELECT NULL)
        OFFSET ${offset} ROWS
        FETCH NEXT ${batchSize} ROWS ONLY
    `);
    
    for (const row of sourceData.recordset) {
        try {
            await bulkInsertData(targetPool, tableName, schema, [row]);
        } catch (error) {
            console.error(`   ⚠️  Failed to insert individual row, skipping...`);
        }
    }
}

async function fixDatabaseIssues() {
    let sourcePool, targetPool;
    
    try {
        console.log('🔌 Connecting to databases for fixing...');
        sourcePool = await sql.connect(sourceConfig);
        targetPool = new sql.ConnectionPool(targetConfig);
        await targetPool.connect();
        console.log('✅ Connected to both databases');
        
        console.log('🔍 Identifying issues...');
        const tables = await getTableList(sourcePool);
        const problematicTables = [];
        
        for (const table of tables) {
            try {
                const verification = await verifyTableData(sourcePool, targetPool, table);
                if (verification.status !== 'MATCH') {
                    problematicTables.push({
                        table,
                        verification
                    });
                }
            } catch (error) {
                console.error(`Error verifying ${table}:`, error.message);
            }
        }
        
        console.log(`\n🔧 Found ${problematicTables.length} tables with issues`);
        
        for (let i = 0; i < problematicTables.length; i++) {
            const { table, verification } = problematicTables[i];
            console.log(`\n[${i + 1}/${problematicTables.length}] Fixing table: ${table}`);
            
            const hasStructureIssues = verification.issues.some(issue => issue.type === 'STRUCTURE');
            const hasDataIssues = verification.issues.some(issue => 
                issue.type === 'ROW_COUNT' || issue.type === 'DATA_CHECKSUM'
            );
            
            if (hasStructureIssues) {
                console.log(`   📋 Fixing structure issues...`);
                const structureDiffs = await compareTableStructure(sourcePool, targetPool, table);
                console.log(`   ⚠️  Structure differences found, consider manual review`);
            }
            
            if (hasDataIssues) {
                await retransferTableData(sourcePool, targetPool, table);
            }
        }
        
        console.log('\n🔍 Running final verification...');
        const finalResults = [];
        let fixedCount = 0;
        
        for (const { table } of problematicTables) {
            const verification = await verifyTableData(sourcePool, targetPool, table);
            finalResults.push(verification);
            
            if (verification.status === 'MATCH') {
                fixedCount++;
                console.log(`✅ ${table}: FIXED`);
            } else {
                console.log(`❌ ${table}: STILL HAS ISSUES`);
                verification.issues.forEach(issue => {
                    console.log(`   - ${issue.type}: ${issue.message}`);
                });
            }
        }
        
        console.log('\n📊 FIX SUMMARY');
        console.log('═'.repeat(50));
        console.log(`✅ Fixed tables: ${fixedCount}`);
        console.log(`❌ Still problematic: ${problematicTables.length - fixedCount}`);
        console.log(`📋 Total processed: ${problematicTables.length}`);
        
        const overallSuccess = fixedCount === problematicTables.length;
        console.log(`\n${overallSuccess ? '🎉 ALL ISSUES FIXED' : '⚠️  SOME ISSUES REMAIN'}`);
        
        return {
            success: overallSuccess,
            fixed: fixedCount,
            remaining: problematicTables.length - fixedCount,
            results: finalResults
        };
        
    } catch (error) {
        console.error('💥 Fix operation failed:', error.message);
        throw error;
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
    fixDatabaseIssues()
        .then(result => {
            process.exit(result.success ? 0 : 1);
        })
        .catch(error => {
            console.error('Fixer failed:', error.message);
            process.exit(1);
        });
}

module.exports = {
    fixDatabaseIssues,
    retransferTableData,
    fixTableStructure
};