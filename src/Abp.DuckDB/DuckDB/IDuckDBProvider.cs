namespace Abp.DuckDB;

/// <summary>
/// DuckDB 查询提供程序基础接口
/// </summary>
public interface IDuckDBProvider : IDuckDBPerformanceMonitor, IDisposable
{
    /// <summary>
    /// 初始化查询提供程序 - 支持简单连接字符串
    /// </summary>
    void Initialize(string connectionString);

    /// <summary>
    /// 使用配置初始化查询提供程序
    /// </summary>
    void Initialize(DuckDBConfiguration configuration);

    /// <summary>
    /// 通过自定义SQL查询数据
    /// </summary>
    Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, params object[] parameters);

    /// <summary>
    /// 获取连接池状态信息
    /// </summary>
    PoolStatus GetConnectionPoolStatus();

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
    void PrewarmEntityMetadata(params Type[] entityTypes);

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
    void PrewarmEntityMetadata(IEnumerable<Type> entityTypes);

    /// <summary>
    /// 手动清理缓存
    /// </summary>
    /// <param name="evictionPercentage">要清除的缓存百分比 (0-100)</param>
    void CleanupCache(int evictionPercentage = 20);

    /// <summary>
    /// 清空所有缓存
    /// </summary>
    void ClearAllCaches();

    /// <summary>
    /// 获取缓存统计信息
    /// </summary>
    string GetCacheStatistics();
}
