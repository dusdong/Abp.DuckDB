using System.Linq.Expressions;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB文件查询提供程序接口
/// </summary>
public interface IDuckDBFileQueryProvider : IDisposable
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
    /// 分析查询计划
    /// </summary>
    Task<string> AnalyzeQueryPlanAsync(string sql);

    /// <summary>
    /// 查询数据
    /// </summary>
    Task<List<TEntity>> QueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 流式查询数据
    /// </summary>
    Task<int> QueryStreamAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        Func<IEnumerable<TEntity>, Task> processAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 流式查询数据，返回异步枚举器
    /// </summary>
    IAsyncEnumerable<TEntity> QueryStreamEnumerableAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 分页查询数据
    /// </summary>
    Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = null,
        bool ascending = true);

    /// <summary>
    /// 统计满足条件的记录数
    /// </summary>
    Task<int> CountAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 批量查询多个不同条件的结果
    /// </summary>
    Task<Dictionary<string, List<TEntity>>> BatchQueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Dictionary<string, Expression<Func<TEntity, bool>>> predicatesMap);

    /// <summary>
    /// 同时获取多个指标
    /// </summary>
    Task<Dictionary<string, decimal>> GetMultipleMetricsAsync<TEntity>(
        IEnumerable<string> filePaths,
        Dictionary<string, string> metricsMap,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 求和
    /// </summary>
    Task<decimal> SumAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 求平均值
    /// </summary>
    Task<decimal> AvgAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 求最小值
    /// </summary>
    Task<decimal> MinAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 求最大值
    /// </summary>
    Task<decimal> MaxAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 通过自定义SQL查询数据
    /// </summary>
    Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, params object[] parameters);

    /// <summary>
    /// 获取性能统计报告
    /// </summary>
    QueryPerformanceMonitor.PerformanceReport GetPerformanceReport();

    /// <summary>
    /// 获取查询类型的性能指标
    /// </summary>
    QueryPerformanceMonitor.QueryMetrics GetMetricsForQueryType(string queryType);

    /// <summary>
    /// 重置性能指标
    /// </summary>
    void ResetPerformanceMetrics();

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
    /// <param name="entityTypes">需要预热的实体类型集合</param>
    void PrewarmEntityMetadata(params Type[] entityTypes);

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
    /// <param name="entityTypes">需要预热的实体类型集合</param>
    void PrewarmEntityMetadata(IEnumerable<Type> entityTypes);

    /// <summary>
    /// 获取连接池状态信息
    /// </summary>
    public PoolStatus GetConnectionPoolStatus();

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
    /// <returns>缓存统计信息字符串</returns>
    string GetCacheStatistics();
}
