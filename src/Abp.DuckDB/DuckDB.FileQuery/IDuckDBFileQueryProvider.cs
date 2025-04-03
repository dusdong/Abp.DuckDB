using System.Linq.Expressions;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB文件查询提供程序接口
/// </summary>
public interface IDuckDbFileQueryProvider : IDuckDBProvider
{
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
}
