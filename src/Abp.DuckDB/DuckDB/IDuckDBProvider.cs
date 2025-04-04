using System.Linq.Expressions;
using DuckDB.NET.Data;

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

    /// <summary>
    /// 记录批处理进度
    /// </summary>
    void RecordBatchProcessing(int itemCount);

    /// <summary>
    /// 分析查询计划
    /// </summary>
    Task<string> AnalyzeQueryPlanAsync(string sql);

    /// <summary>
    /// 获取性能报告
    /// </summary>
    QueryPerformanceReport GetPerformanceReport();

    /// <summary>
    /// 获取查询类型的性能指标
    /// </summary>
    QueryPerformanceMetrics GetMetricsForQueryType(string queryType);

    /// <summary>
    /// 重置性能指标
    /// </summary>
    void ResetPerformanceMetrics();

    /// <summary>
    /// 获取最近的查询执行日志
    /// </summary>
    List<QueryExecutionLog> GetRecentExecutions(int count = 100);

    /// <summary>
    /// 清除最近的执行日志
    /// </summary>
    void ClearExecutionLogs();

    // 从 IDuckDBProviderAdvanced 合并的方法

    /// <summary>
    /// 获取原始DuckDB连接以执行高级操作
    /// </summary>
    DuckDBConnection GetDuckDBConnection();

    /// <summary>
    /// 执行非查询SQL语句
    /// </summary>
    Task<int> ExecuteNonQueryAsync(string sql, params object[] parameters);

    /// <summary>
    /// 带有限制和偏移的分页查询
    /// </summary>
    Task<List<TEntity>> QueryWithLimitOffsetAsync<TEntity>(
        Expression<Func<TEntity, bool>> predicate = null,
        int limit = 1000,
        int offset = 0,
        string orderByColumn = null,
        bool ascending = true);

    /// <summary>
    /// 应用优化设置
    /// </summary>
    Task ApplyOptimizationAsync();
}
