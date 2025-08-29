# 🚨 DISASTER RECOVERY GUIDE 🚨

## Emergency Database Migration from RDS to RDS

This guide will help you migrate your database from one RDS instance to another in case of data loss or corruption.

## 📋 Prerequisites

### 1. Environment Setup
- Node.js installed (version 14+)
- Access to both RDS instances
- Network connectivity to both databases
- Sufficient disk space for export files

### 2. RDS Configuration
- Both RDS instances must be running
- Security groups must allow connections on port 1433
- Database users must have appropriate permissions:
  - `SELECT` permissions on all tables (source)
  - `CREATE`, `ALTER`, `DROP` permissions (target)

### 3. AWS Considerations
- Ensure sufficient RDS instance capacity
- Monitor CloudWatch metrics during migration
- Consider temporary instance scaling for large databases

## ⚡ Quick Start (Emergency Mode)

### Step 1: Configure Environment
```bash
# Copy the disaster recovery template
cp .env.disaster-recovery .env

# Edit .env with your RDS credentials
nano .env
```

### Step 2: Configure Your Databases
Edit the `.env` file with your actual RDS information:

```env
# Source RDS (where your data currently exists)
SOURCE_DB_SERVER=your-source-instance.rds.amazonaws.com
SOURCE_DB_DATABASE=your_database_name
SOURCE_DB_USER=your_username
SOURCE_DB_PASSWORD=your_password

# Target RDS (where you want to restore data)
TARGET_DB_SERVER=your-target-instance.rds.amazonaws.com
TARGET_DB_DATABASE=your_database_name
TARGET_DB_USER=your_username
TARGET_DB_PASSWORD=your_password
```

### Step 3: Run Disaster Recovery
```bash
# Run the complete migration
npm run disaster-recovery

# Or use the shorter command
npm run migrate
```

## 📊 Detailed Migration Process

### Phase 1: Validation & Preparation
1. **Connection Test**: Verifies connectivity to both databases
2. **Source Analysis**: Analyzes database size and table structure
3. **Target Backup**: Creates backup of existing target data (optional)
4. **Resource Check**: Validates available disk space and memory

### Phase 2: Data Export
1. **Schema Export**: Exports all table structures, constraints, and relationships
2. **Data Export**: Exports table data with automatic chunking for large tables
3. **Progress Monitoring**: Real-time progress updates and error handling
4. **Memory Management**: Automatic memory optimization for large datasets

### Phase 3: Data Import
1. **Target Preparation**: Prepares target database for import
2. **Schema Recreation**: Recreates all tables with proper structure
3. **Data Import**: Imports data with batch processing and error recovery
4. **Constraint Creation**: Adds foreign keys and other constraints

### Phase 4: Validation & Reporting
1. **Data Validation**: Compares source and target data integrity
2. **Row Count Verification**: Ensures all data was migrated correctly
3. **Migration Report**: Generates detailed migration summary
4. **Cleanup**: Removes temporary export files

## 🔧 Configuration Options

### Memory Management (for large databases)
```env
MAX_ROWS_PER_FILE=50000      # Split large tables into chunks
MEMORY_LIMIT=1000000000     # 1GB memory limit
ENABLE_STREAMING=true       # Enable streaming for very large tables
```

### Migration Control
```env
VALIDATE_DATA=true          # Validate data integrity after migration
CREATE_BACKUP=true          # Backup target data before migration
MAX_RETRIES=3               # Number of retry attempts on failure
RETRY_DELAY=5000            # Delay between retries (ms)
```

### Performance Tuning
```env
SOURCE_DB_CONNECTION_TIMEOUT=60000   # 60 second connection timeout
SOURCE_DB_REQUEST_TIMEOUT=120000     # 2 minute query timeout
```

## 📈 Monitoring & Troubleshooting

### Real-time Monitoring
The migration script provides detailed logging:
- Connection status and database versions
- Table-by-table progress with row counts
- Memory usage and performance metrics
- Error details and recovery actions

### Common Issues & Solutions

#### Connection Issues
```bash
# Test connectivity to source
nslookup your-source-instance.rds.amazonaws.com

# Test connectivity to target
nslookup your-target-instance.rds.amazonaws.com

# Verify security group rules in AWS console
```

#### Memory Issues
```bash
# For very large databases, increase Node.js memory
node --max-old-space-size=8192 disaster-recovery.js

# Or adjust chunk size for smaller memory footprint
MAX_ROWS_PER_FILE=25000
MEMORY_LIMIT=500000000
```

#### Timeout Issues
```bash
# Increase timeouts for slow networks/large datasets
SOURCE_DB_CONNECTION_TIMEOUT=120000
SOURCE_DB_REQUEST_TIMEOUT=300000
TARGET_DB_CONNECTION_TIMEOUT=120000
TARGET_DB_REQUEST_TIMEOUT=300000
```

#### Permission Issues
Ensure your database users have these permissions:

**Source Database User:**
- `SELECT` on all tables
- `SELECT` on `INFORMATION_SCHEMA` views

**Target Database User:**
- `CREATE DATABASE` (if creating new database)
- `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
- `INSERT`, `UPDATE`, `DELETE` on all tables
- `CREATE INDEX`, `ALTER INDEX`

## 📋 Migration Checklist

### Pre-Migration
- [ ] Backup source database (if possible)
- [ ] Verify target RDS instance is running
- [ ] Check security group configurations
- [ ] Test database connectivity
- [ ] Verify user permissions
- [ ] Ensure sufficient disk space
- [ ] Review migration configuration

### During Migration
- [ ] Monitor AWS CloudWatch metrics
- [ ] Watch for connection timeouts
- [ ] Check disk space usage
- [ ] Monitor memory usage
- [ ] Keep emergency contacts available

### Post-Migration
- [ ] Verify data integrity
- [ ] Test application connectivity
- [ ] Update connection strings
- [ ] Monitor application performance
- [ ] Plan rollback if needed

## 🔄 Rollback Procedures

### If Migration Fails
1. **Stop the migration** (Ctrl+C)
2. **Check the logs** for specific error details
3. **Review the backup** created before migration
4. **Restore from backup** if target data was corrupted

### Emergency Rollback
```bash
# If you need to restore the target from backup
cp .env.backup .env  # Use backup configuration
npm run import      # Import from backup files
```

## 📞 Support & Emergency Contacts

### During Business Hours
- Database Administrator
- AWS Support
- Application Support Team

### After Hours / Emergency
- On-call DBA
- AWS Premium Support
- Infrastructure Team

## 📊 Performance Expectations

### Small Database (< 1GB)
- **Duration**: 5-15 minutes
- **Memory Usage**: 100-500MB
- **Network Transfer**: Minimal

### Medium Database (1GB - 10GB)
- **Duration**: 15-60 minutes
- **Memory Usage**: 500MB - 2GB
- **Network Transfer**: Moderate

### Large Database (10GB+)
- **Duration**: 1-4+ hours
- **Memory Usage**: 2-8GB+
- **Network Transfer**: Significant
- **Recommendation**: Scale RDS instances during migration

## 🚨 Emergency Commands

```bash
# Quick migration (minimal validation)
CREATE_BACKUP=false VALIDATE_DATA=false npm run migrate

# Memory-optimized for large databases
MAX_ROWS_PER_FILE=25000 MEMORY_LIMIT=500000000 npm run migrate

# Debug mode with verbose logging
DEBUG=* npm run migrate

# Stop migration gracefully
# Press Ctrl+C once for graceful shutdown
# Press Ctrl+C twice for immediate termination
```

## 📝 Post-Migration Tasks

1. **Update Application Configuration**
   - Update connection strings to point to new RDS instance
   - Test application functionality
   - Monitor for performance issues

2. **Database Maintenance**
   - Update statistics: `EXEC sp_updatestats`
   - Rebuild indexes if needed
   - Verify backup jobs are configured

3. **Monitoring Setup**
   - Configure CloudWatch alarms
   - Set up automated backups
   - Monitor query performance

4. **Documentation Update**
   - Update runbooks with new instance information
   - Document any issues encountered during migration
   - Update disaster recovery procedures

---

## ⚠️ Important Notes

- **Test First**: Always test the migration process with a small dataset first
- **Monitor Resources**: Keep an eye on AWS RDS metrics during migration
- **Have a Plan B**: Ensure you have backup recovery options
- **Document Everything**: Keep detailed records of the migration process
- **Communicate**: Inform stakeholders about the migration timeline and potential downtime

**Remember**: This is a critical operation. Take your time, verify each step, and don't hesitate to ask for help if you encounter issues.