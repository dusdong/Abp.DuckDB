using System.Collections.Concurrent;
using Castle.Core.Logging;

namespace Abp.DuckDB.MultiTenant;

/// <summary>
/// 租户数据库管理器，负责租户数据库路径和连接字符串管理
/// </summary>
public class TenantDatabaseManager
{
    private readonly string _basePath;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, string> _connectionStringsCache = new ConcurrentDictionary<string, string>();
    private readonly ConcurrentDictionary<string, TenantDatabaseInfo> _tenantInfoCache = new ConcurrentDictionary<string, TenantDatabaseInfo>();

    /// <summary>
    /// 数据库文件扩展名
    /// </summary>
    public string DatabaseFileExtension { get; set; } = ".db";

    /// <summary>
    /// 创建租户数据库管理器实例
    /// </summary>
    /// <param name="basePath">租户数据库文件的基础目录</param>
    /// <param name="logger">日志记录器</param>
    public TenantDatabaseManager(string basePath, ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(basePath))
            throw new ArgumentNullException(nameof(basePath));

        _basePath = Path.GetFullPath(basePath);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // 确保基础目录存在
        if (!Directory.Exists(_basePath))
        {
            try
            {
                Directory.CreateDirectory(_basePath);
                _logger.Info($"创建租户数据库目录: {_basePath}");
            }
            catch (Exception ex)
            {
                _logger.Error($"创建租户数据库目录失败: {ex.Message}", ex);
                throw;
            }
        }
    }

    /// <summary>
    /// 获取租户数据库文件路径
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>数据库文件完整路径</returns>
    public string GetDatabaseFilePath(string tenantId)
    {
        ValidateTenantId(tenantId);

        // 子目录组织：使用前两个字符作为子目录，避免单目录下文件过多
        string subDir = tenantId.Length >= 2 ? tenantId.Substring(0, 2) : tenantId;
        string tenantDir = Path.Combine(_basePath, subDir);

        // 确保子目录存在
        if (!Directory.Exists(tenantDir))
        {
            Directory.CreateDirectory(tenantDir);
        }

        return Path.Combine(tenantDir, $"{tenantId}{DatabaseFileExtension}");
    }

    /// <summary>
    /// 获取租户数据库连接字符串
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>DuckDB连接字符串</returns>
    public string GetConnectionString(string tenantId)
    {
        return _connectionStringsCache.GetOrAdd(tenantId, id =>
        {
            string dbPath = GetDatabaseFilePath(id);
            return $"Data Source={dbPath}";
        });
    }

    /// <summary>
    /// 检查租户数据库是否存在
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>如果存在则返回true</returns>
    public bool TenantDatabaseExists(string tenantId)
    {
        return File.Exists(GetDatabaseFilePath(tenantId));
    }

    /// <summary>
    /// 获取所有租户ID列表
    /// </summary>
    /// <returns>租户ID集合</returns>
    public IEnumerable<string> GetAllTenantIds()
    {
        var result = new List<string>();

        foreach (var dir in Directory.GetDirectories(_basePath))
        {
            foreach (var file in Directory.GetFiles(dir, $"*{DatabaseFileExtension}"))
            {
                string fileName = Path.GetFileNameWithoutExtension(file);
                result.Add(fileName);
            }
        }

        return result;
    }

    /// <summary>
    /// 获取租户数据库信息
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>租户数据库信息</returns>
    public TenantDatabaseInfo GetTenantInfo(string tenantId)
    {
        return _tenantInfoCache.GetOrAdd(tenantId, id =>
        {
            string dbPath = GetDatabaseFilePath(id);
            bool exists = File.Exists(dbPath);

            DateTime createdDate = DateTime.MinValue;
            DateTime lastModified = DateTime.MinValue;
            long fileSize = 0;

            if (exists)
            {
                var fileInfo = new FileInfo(dbPath);
                createdDate = fileInfo.CreationTime;
                lastModified = fileInfo.LastWriteTime;
                fileSize = fileInfo.Length;
            }

            return new TenantDatabaseInfo
            {
                TenantId = id,
                DatabasePath = dbPath,
                Exists = exists,
                CreatedDate = createdDate,
                LastModifiedDate = lastModified,
                SizeInBytes = fileSize
            };
        });
    }

    /// <summary>
    /// 清除租户信息缓存
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    public void ClearCache(string tenantId)
    {
        _connectionStringsCache.TryRemove(tenantId, out _);
        _tenantInfoCache.TryRemove(tenantId, out _);
    }

    /// <summary>
    /// 验证租户ID是否有效
    /// </summary>
    private void ValidateTenantId(string tenantId)
    {
        if (string.IsNullOrWhiteSpace(tenantId))
        {
            throw new ArgumentNullException(nameof(tenantId));
        }

        // 只允许字母、数字、下划线和短横线
        if (!System.Text.RegularExpressions.Regex.IsMatch(tenantId, @"^[a-zA-Z0-9_\-]+$"))
        {
            throw new ArgumentException("租户ID只能包含字母、数字、下划线和短横线", nameof(tenantId));
        }
    }
}

/// <summary>
/// 租户数据库信息
/// </summary>
public class TenantDatabaseInfo
{
    /// <summary>
    /// 租户ID
    /// </summary>
    public string TenantId { get; set; }

    /// <summary>
    /// 数据库文件路径
    /// </summary>
    public string DatabasePath { get; set; }

    /// <summary>
    /// 数据库是否存在
    /// </summary>
    public bool Exists { get; set; }

    /// <summary>
    /// 创建日期
    /// </summary>
    public DateTime CreatedDate { get; set; }

    /// <summary>
    /// 最后修改日期
    /// </summary>
    public DateTime LastModifiedDate { get; set; }

    /// <summary>
    /// 文件大小（字节）
    /// </summary>
    public long SizeInBytes { get; set; }

    /// <summary>
    /// 格式化的文件大小
    /// </summary>
    public string FormattedSize
    {
        get
        {
            if (SizeInBytes < 1024)
                return $"{SizeInBytes} B";
            if (SizeInBytes < 1024 * 1024)
                return $"{SizeInBytes / 1024.0:F2} KB";
            if (SizeInBytes < 1024 * 1024 * 1024)
                return $"{SizeInBytes / (1024.0 * 1024):F2} MB";
            return $"{SizeInBytes / (1024.0 * 1024 * 1024):F2} GB";
        }
    }
}
