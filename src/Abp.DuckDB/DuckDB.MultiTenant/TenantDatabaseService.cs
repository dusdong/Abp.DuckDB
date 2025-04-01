using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.MultiTenant;

/// <summary>
/// 租户数据库服务，负责租户数据库生命周期管理
/// </summary>
public class TenantDatabaseService : IDisposable
{
    private readonly TenantDatabaseManager _dbManager;
    private readonly DuckDBConnectionPool _connectionPool;
    private readonly ILogger _logger;
    private bool _disposed;

    /// <summary>
    /// 创建租户数据库服务实例
    /// </summary>
    /// <param name="dbManager">租户数据库管理器</param>
    /// <param name="connectionPool">连接池</param>
    /// <param name="logger">日志记录器</param>
    public TenantDatabaseService(
        TenantDatabaseManager dbManager,
        DuckDBConnectionPool connectionPool,
        ILogger logger)
    {
        _dbManager = dbManager ?? throw new ArgumentNullException(nameof(dbManager));
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// 初始化租户数据库（如果不存在则创建）
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>成功创建或已存在返回true，否则返回false</returns>
    public async Task<bool> InitializeTenantDatabaseAsync(string tenantId)
    {
        try
        {
            // 检查租户数据库是否已存在
            if (_dbManager.TenantDatabaseExists(tenantId))
            {
                _logger.Debug($"租户 {tenantId} 数据库已存在，跳过初始化");
                return true;
            }

            string dbPath = _dbManager.GetDatabaseFilePath(tenantId);
            string connectionString = _dbManager.GetConnectionString(tenantId);

            _logger.Info($"正在初始化租户 {tenantId} 数据库，路径: {dbPath}");

            // 创建数据库文件
            using (var connection = new DuckDBConnection(connectionString))
            {
                await connection.OpenAsync();
                _logger.Info($"租户 {tenantId} 数据库创建成功");

                // 初始化租户元数据表
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                            CREATE TABLE IF NOT EXISTS tenant_metadata (
                                key VARCHAR NOT NULL PRIMARY KEY,
                                value VARCHAR,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            );
                            
                            INSERT INTO tenant_metadata (key, value) VALUES 
                            ('tenant_id', $tenantId),
                            ('created_at', $createdAt),
                            ('schema_version', '1.0');
                        ";
                        
                    command.Parameters.Add(new DuckDBParameter { ParameterName = "tenantId", Value = tenantId });
                    command.Parameters.Add(new DuckDBParameter { ParameterName = "createdAt", Value = DateTime.UtcNow.ToString("o") });

                    await command.ExecuteNonQueryAsync();
                }
            }

            _logger.Info($"租户 {tenantId} 初始化完成");
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"初始化租户 {tenantId} 数据库失败: {ex.Message}", ex);
                
            // 清理失败的初始化
            try
            {
                string dbPath = _dbManager.GetDatabaseFilePath(tenantId);
                if (File.Exists(dbPath))
                {
                    File.Delete(dbPath);
                    _logger.Info($"已删除失败的租户 {tenantId} 数据库文件");
                }
            }
            catch (Exception cleanupEx)
            {
                _logger.Error($"清理失败的租户 {tenantId} 数据库文件时出错: {cleanupEx.Message}", cleanupEx);
            }
                
            return false;
        }
    }

    /// <summary>
    /// 执行租户数据库架构迁移
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <param name="targetVersion">目标架构版本</param>
    /// <param name="migrationScripts">迁移脚本集合，键为版本号</param>
    /// <returns>迁移成功返回true，否则返回false</returns>
    public async Task<bool> MigrateTenantDatabaseAsync(
        string tenantId, 
        string targetVersion,
        IDictionary<string, string> migrationScripts)
    {
        if (!_dbManager.TenantDatabaseExists(tenantId))
        {
            _logger.Error($"租户 {tenantId} 数据库不存在，无法执行迁移");
            return false;
        }

        try
        {
            using (var pooledConnection = await _connectionPool.GetConnectionAsync(tenantId))
            using (var connection = pooledConnection.Connection)
            {
                // 获取当前架构版本
                string currentVersion = await GetSchemaVersionAsync(connection);
                _logger.Info($"租户 {tenantId} 当前架构版本: {currentVersion}，目标版本: {targetVersion}");

                // 如果已是目标版本则跳过
                if (string.Compare(currentVersion, targetVersion) >= 0)
                {
                    _logger.Info($"租户 {tenantId} 已是最新版本，跳过迁移");
                    return true;
                }

                // 确定需要执行的迁移脚本
                var versionsToApply = migrationScripts.Keys
                    .Where(v => string.Compare(v, currentVersion) > 0 && 
                                string.Compare(v, targetVersion) <= 0)
                    .OrderBy(v => v)
                    .ToList();

                if (!versionsToApply.Any())
                {
                    _logger.Warn($"租户 {tenantId} 没有找到适用的迁移脚本，当前版本: {currentVersion}，目标版本: {targetVersion}");
                    return false;
                }

                // 执行事务迁移
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        foreach (var version in versionsToApply)
                        {
                            string script = migrationScripts[version];
                            _logger.Info($"正在应用租户 {tenantId} 的迁移脚本，版本: {version}");

                            using (var command = connection.CreateCommand())
                            {
                                command.Transaction = transaction;
                                command.CommandText = script;
                                await command.ExecuteNonQueryAsync();
                            }
                        }

                        // 更新架构版本
                        using (var command = connection.CreateCommand())
                        {
                            command.Transaction = transaction;
                            command.CommandText = @"
                                    UPDATE tenant_metadata 
                                    SET value = $version, updated_at = $updatedAt 
                                    WHERE key = 'schema_version';
                                ";
                                
                            command.Parameters.Add(new DuckDBParameter { ParameterName = "version", Value = targetVersion });
                            command.Parameters.Add(new DuckDBParameter { ParameterName = "updatedAt", Value = DateTime.UtcNow.ToString("o") });

                            await command.ExecuteNonQueryAsync();
                        }

                        await transaction.CommitAsync();
                        _logger.Info($"租户 {tenantId} 成功迁移到版本 {targetVersion}");
                        return true;
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        _logger.Error($"租户 {tenantId} 迁移过程中发生错误: {ex.Message}", ex);
                        return false;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"租户 {tenantId} 迁移准备阶段发生错误: {ex.Message}", ex);
            return false;
        }
    }

    /// <summary>
    /// 备份租户数据库
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <param name="backupPath">备份文件路径，如为null，则使用默认路径</param>
    /// <returns>备份文件的完整路径</returns>
    public async Task<string> BackupTenantDatabaseAsync(string tenantId, string backupPath = null)
    {
        if (!_dbManager.TenantDatabaseExists(tenantId))
        {
            throw new InvalidOperationException($"租户 {tenantId} 数据库不存在，无法备份");
        }

        string sourcePath = _dbManager.GetDatabaseFilePath(tenantId);
            
        // 如果未指定备份路径，使用默认路径
        if (string.IsNullOrEmpty(backupPath))
        {
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            backupPath = Path.Combine(
                Path.GetDirectoryName(sourcePath),
                $"{tenantId}_backup_{timestamp}{_dbManager.DatabaseFileExtension}");
        }

        try
        {
            // 确保备份目录存在
            Directory.CreateDirectory(Path.GetDirectoryName(backupPath));
                
            // 获取连接并备份
            using (var pooledConnection = await _connectionPool.GetConnectionAsync(tenantId))
            {
                // 执行COPY DATABASE命令
                using (var command = pooledConnection.CreateCommand())
                {
                    command.CommandText = @"
                            EXPORT DATABASE $backupPath;
                        ";
                        
                    command.Parameters.Add(new DuckDBParameter { ParameterName = "backupPath", Value = backupPath });
                        
                    _logger.Info($"开始备份租户 {tenantId} 数据库到 {backupPath}");
                    await command.ExecuteNonQueryAsync();
                    _logger.Info($"租户 {tenantId} 数据库备份完成，保存到: {backupPath}");
                }
            }

            return backupPath;
        }
        catch (Exception ex)
        {
            _logger.Error($"备份租户 {tenantId} 数据库失败: {ex.Message}", ex);
                
            // 清理失败的备份文件
            try
            {
                if (File.Exists(backupPath))
                {
                    File.Delete(backupPath);
                    _logger.Info($"已删除失败的备份文件: {backupPath}");
                }
            }
            catch { }
                
            throw;
        }
    }

    /// <summary>
    /// 恢复租户数据库
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <param name="backupPath">备份文件路径</param>
    /// <returns>恢复成功返回true，否则返回false</returns>
    public async Task<bool> RestoreTenantDatabaseAsync(string tenantId, string backupPath)
    {
        if (string.IsNullOrEmpty(backupPath) || !File.Exists(backupPath))
        {
            throw new ArgumentException($"备份文件不存在: {backupPath}", nameof(backupPath));
        }

        try
        {
            // 首先清理现有连接
            await _connectionPool.ClearPoolAsync(tenantId);
                
            string dbPath = _dbManager.GetDatabaseFilePath(tenantId);
            string connectionString = _dbManager.GetConnectionString(tenantId);
                
            // 如果数据库已存在，先删除它
            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
                _logger.Info($"已删除租户 {tenantId} 的现有数据库文件");
            }
                
            // 创建新连接
            using (var connection = new DuckDBConnection(connectionString))
            {
                await connection.OpenAsync();
                    
                // 执行IMPORT DATABASE命令
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                            IMPORT DATABASE $backupPath;
                        ";
                        
                    command.Parameters.Add(new DuckDBParameter { ParameterName = "backupPath", Value = backupPath });
                        
                    _logger.Info($"开始从 {backupPath} 恢复租户 {tenantId} 数据库");
                    await command.ExecuteNonQueryAsync();
                    _logger.Info($"租户 {tenantId} 数据库恢复完成");
                }
            }
                
            // 刷新租户信息缓存
            _dbManager.ClearCache(tenantId);
                
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"恢复租户 {tenantId} 数据库失败: {ex.Message}", ex);
            return false;
        }
    }

    /// <summary>
    /// 删除租户数据库
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <param name="createBackup">是否创建备份</param>
    /// <returns>删除成功返回true，否则返回false</returns>
    public async Task<bool> DeleteTenantDatabaseAsync(string tenantId, bool createBackup = true)
    {
        if (!_dbManager.TenantDatabaseExists(tenantId))
        {
            _logger.Warn($"租户 {tenantId} 数据库不存在，无需删除");
            return true;
        }

        try
        {
            string dbPath = _dbManager.GetDatabaseFilePath(tenantId);
            string backupPath = null;
                
            // 先清理连接池
            await _connectionPool.ClearPoolAsync(tenantId);
                
            // 创建备份
            if (createBackup)
            {
                backupPath = await BackupTenantDatabaseAsync(tenantId);
                _logger.Info($"已为租户 {tenantId} 创建删除前备份: {backupPath}");
            }
                
            // 删除数据库文件
            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
                _logger.Info($"已删除租户 {tenantId} 数据库文件: {dbPath}");
            }
                
            // 清理租户信息缓存
            _dbManager.ClearCache(tenantId);
                
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"删除租户 {tenantId} 数据库失败: {ex.Message}", ex);
            return false;
        }
    }

    /// <summary>
    /// 获取架构版本
    /// </summary>
    private async Task<string> GetSchemaVersionAsync(DuckDBConnection connection)
    {
        using (var command = connection.CreateCommand())
        {
            // 检查元数据表是否存在
            command.CommandText = @"
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_name = 'tenant_metadata'
                    );
                ";
                
            var tableExists = (bool)await command.ExecuteScalarAsync();
                
            if (!tableExists)
            {
                return "0.0";
            }
                
            // 查询架构版本
            command.CommandText = @"
                    SELECT value 
                    FROM tenant_metadata 
                    WHERE key = 'schema_version'
                    LIMIT 1;
                ";
                
            var result = await command.ExecuteScalarAsync();
            return result?.ToString() ?? "0.0";
        }
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            // 无需额外的清理，由connectionPool管理连接释放
        }
    }
}
