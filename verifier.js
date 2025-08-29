const sql = require('mssql');
const { sourceConfig, targetConfig } = require('./config');
const { getTableList, getTableSchema } = require('./database');
const { executeWithRetry, formatProgress } = require('./utils');

class VerificationError extends Error {
    constructor(message, table = null, type = null) {
        super(message);
        this.name = 'VerificationError';
        this.table = table;
        this.type = type;
    }
}

async function compareTableStructure(sourcePool, targetPool, tableName) {
    const sourceSchema = await getTableSchema(sourcePool, tableName);
    const targetSchema = await getTableSchema(targetPool, tableName);
    
    const differences = [];
    
    if (sourceSchema.length !== targetSchema.length) {
        differences.push(`Column count mismatch: source ${sourceSchema.length}, target ${targetSchema.length}`);
    }
    
    const sourceColumns = new Map(sourceSchema.map(col => [col.COLUMN_NAME, col]));
    const targetColumns = new Map(targetSchema.map(col => [col.COLUMN_NAME, col]));
    
    for (const [colName, sourceCol] of sourceColumns) {
        const targetCol = targetColumns.get(colName);
        
        if (!targetCol) {
            differences.push(`Missing column in target: ${colName}`);
            continue;
        }
        
        if (sourceCol.DATA_TYPE !== targetCol.DATA_TYPE) {
            differences.push(`Column ${colName} type mismatch: source ${sourceCol.DATA_TYPE}, target ${targetCol.DATA_TYPE}`);
        }
        
        if (sourceCol.IS_NULLABLE !== targetCol.IS_NULLABLE) {
            differences.push(`Column ${colName} nullable mismatch: source ${sourceCol.IS_NULLABLE}, target ${targetCol.IS_NULLABLE}`);
        }
        
        if (sourceCol.CHARACTER_MAXIMUM_LENGTH !== targetCol.CHARACTER_MAXIMUM_LENGTH) {
            differences.push(`Column ${colName} length mismatch: source ${sourceCol.CHARACTER_MAXIMUM_LENGTH}, target ${targetCol.CHARACTER_MAXIMUM_LENGTH}`);
        }
    }
    
    for (const colName of targetColumns.keys()) {
        if (!sourceColumns.has(colName)) {
            differences.push(`Extra column in target: ${colName}`);
        }
    }
    
    return differences;
}

async function compareTableRowCounts(sourcePool, targetPool, tableName) {
    const sourceCountResult = await sourcePool.request().query(`SELECT COUNT(*) as count FROM [${tableName}]`);
    const targetCountResult = await targetPool.request().query(`SELECT COUNT(*) as count FROM [${tableName}]`);
    
    const sourceCount = sourceCountResult.recordset[0].count;
    const targetCount = targetCountResult.recordset[0].count;
    
    return {
        sourceCount,
        targetCount,
        match: sourceCount === targetCount
    };
}

async function calculateTableChecksum(pool, tableName, schema) {
    const columnList = schema
        .filter(col => !['timestamp', 'rowversion'].includes(col.DATA_TYPE.toLowerCase()))
        .map(col => {
            if (['datetime', 'datetime2', 'smalldatetime'].includes(col.DATA_TYPE.toLowerCase())) {
                return `ISNULL(CONVERT(varchar(23), [${col.COLUMN_NAME}], 121), 'NULL')`;
            }
            return `ISNULL(CAST([${col.COLUMN_NAME}] AS NVARCHAR(MAX)), 'NULL')`;
        })
        .join(" + '|' + ");
    
    if (!columnList) {
        return { checksum: 0, rowCount: 0 };
    }
    
    const query = `
        SELECT 
            COUNT(*) as row_count,
            CHECKSUM_AGG(CHECKSUM(${columnList})) as checksum
        FROM [${tableName}]
    `;
    
    const result = await pool.request().query(query);
    return {
        checksum: result.recordset[0].checksum || 0,
        rowCount: result.recordset[0].row_count
    };
}

async function verifyTableData(sourcePool, targetPool, tableName) {
    console.log(`🔍 Verifying table: ${tableName}`);
    
    const issues = [];
    
    try {
        const structureDiffs = await compareTableStructure(sourcePool, targetPool, tableName);
        if (structureDiffs.length > 0) {
            issues.push(...structureDiffs.map(diff => ({ type: 'STRUCTURE', message: diff })));
        }
        
        const rowCountComparison = await compareTableRowCounts(sourcePool, targetPool, tableName);
        if (!rowCountComparison.match) {
            issues.push({
                type: 'ROW_COUNT',
                message: `Row count mismatch: source ${rowCountComparison.sourceCount}, target ${rowCountComparison.targetCount}`
            });
        }
        
        if (rowCountComparison.sourceCount > 0 && structureDiffs.length === 0) {
            const sourceSchema = await getTableSchema(sourcePool, tableName);
            
            const sourceChecksum = await calculateTableChecksum(sourcePool, tableName, sourceSchema);
            const targetChecksum = await calculateTableChecksum(targetPool, tableName, sourceSchema);
            
            if (sourceChecksum.checksum !== targetChecksum.checksum) {
                issues.push({
                    type: 'DATA_CHECKSUM',
                    message: `Data checksum mismatch: source ${sourceChecksum.checksum}, target ${targetChecksum.checksum}`
                });
            }
        }
        
        return {
            table: tableName,
            status: issues.length === 0 ? 'MATCH' : 'MISMATCH',
            rowCount: rowCountComparison.sourceCount,
            issues
        };
        
    } catch (error) {
        return {
            table: tableName,
            status: 'ERROR',
            rowCount: 0,
            issues: [{ type: 'ERROR', message: error.message }]
        };
    }
}

async function verifyDatabases() {
    let sourcePool, targetPool;
    
    try {
        console.log('🔌 Connecting to databases for verification...');
        sourcePool = await sql.connect(sourceConfig);
        targetPool = new sql.ConnectionPool(targetConfig);
        await targetPool.connect();
        console.log('✅ Connected to both databases');
        
        console.log('📋 Getting table lists...');
        const sourceTables = await getTableList(sourcePool);
        const targetTables = await getTableList(targetPool);
        
        const sourceTableSet = new Set(sourceTables);
        const targetTableSet = new Set(targetTables);
        
        const missingInTarget = sourceTables.filter(table => !targetTableSet.has(table));
        const extraInTarget = targetTables.filter(table => !sourceTableSet.has(table));
        
        if (missingInTarget.length > 0) {
            console.log(`❌ Tables missing in target: ${missingInTarget.join(', ')}`);
        }
        
        if (extraInTarget.length > 0) {
            console.log(`⚠️  Extra tables in target: ${extraInTarget.join(', ')}`);
        }
        
        const commonTables = sourceTables.filter(table => targetTableSet.has(table));
        console.log(`🔍 Verifying ${commonTables.length} common tables...`);
        
        const results = [];
        let matchCount = 0;
        let mismatchCount = 0;
        let errorCount = 0;
        
        for (let i = 0; i < commonTables.length; i++) {
            const table = commonTables[i];
            console.log(`[${i + 1}/${commonTables.length}] ${table}`);
            
            const result = await verifyTableData(sourcePool, targetPool, table);
            results.push(result);
            
            if (result.status === 'MATCH') {
                matchCount++;
                console.log(`   ✅ MATCH (${result.rowCount.toLocaleString()} rows)`);
            } else if (result.status === 'MISMATCH') {
                mismatchCount++;
                console.log(`   ❌ MISMATCH (${result.rowCount.toLocaleString()} rows)`);
                result.issues.forEach(issue => {
                    console.log(`      - ${issue.type}: ${issue.message}`);
                });
            } else {
                errorCount++;
                console.log(`   💥 ERROR`);
                result.issues.forEach(issue => {
                    console.log(`      - ${issue.message}`);
                });
            }
        }
        
        console.log('\n📊 VERIFICATION SUMMARY');
        console.log('═'.repeat(50));
        console.log(`✅ Matching tables: ${matchCount}`);
        console.log(`❌ Mismatched tables: ${mismatchCount}`);
        console.log(`💥 Error tables: ${errorCount}`);
        console.log(`📋 Total verified: ${commonTables.length}`);
        
        if (missingInTarget.length > 0) {
            console.log(`❌ Missing in target: ${missingInTarget.length}`);
        }
        
        if (extraInTarget.length > 0) {
            console.log(`⚠️  Extra in target: ${extraInTarget.length}`);
        }
        
        const overallSuccess = mismatchCount === 0 && errorCount === 0 && missingInTarget.length === 0;
        console.log(`\n${overallSuccess ? '🎉 VERIFICATION PASSED' : '❌ VERIFICATION FAILED'}`);
        
        return {
            success: overallSuccess,
            summary: {
                matching: matchCount,
                mismatched: mismatchCount,
                errors: errorCount,
                missingInTarget: missingInTarget.length,
                extraInTarget: extraInTarget.length
            },
            results
        };
        
    } catch (error) {
        console.error('💥 Verification failed:', error.message);
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
    verifyDatabases()
        .then(result => {
            process.exit(result.success ? 0 : 1);
        })
        .catch(error => {
            console.error('Verification failed:', error.message);
            process.exit(1);
        });
}

module.exports = {
    verifyDatabases,
    verifyTableData,
    compareTableStructure,
    compareTableRowCounts,
    calculateTableChecksum
};