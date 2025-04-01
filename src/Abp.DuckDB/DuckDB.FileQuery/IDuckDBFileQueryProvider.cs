using System.Linq.Expressions;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// 归档查询提供程序接口
/// </summary>
public interface IDuckDBFileQueryProvider : IDisposable
{
    /// <summary>
    /// 初始化查询提供程序
    /// </summary>
    void Initialize(string connectionString);

    /// <summary>
    /// 查询特定类型的存档数据
    /// </summary>
    Task<List<TEntity>> QueryAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 流式查询特定类型的存档数据，适用于大结果集
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="filePaths">文件路径列表</param>
    /// <param name="predicate">过滤条件表达式</param>
    /// <param name="batchSize">每批处理的数据量</param>
    /// <param name="processAction">处理每批数据的委托</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>处理的总记录数</returns>
    Task<int> QueryStreamAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        Func<IEnumerable<TEntity>, Task> processAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 流式查询特定类型的存档数据，返回一个异步枚举器，适用于大结果集
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="filePaths">文件路径列表</param>
    /// <param name="predicate">过滤条件表达式</param>
    /// <param name="batchSize">分批大小</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步枚举结果</returns>
    IAsyncEnumerable<TEntity> QueryStreamEnumerableAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 分页查询特定类型的存档数据
    /// </summary>
    Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = null,
        bool ascending = true);

    /// <summary>
    /// 批量查询多个不同条件的结果（批处理优化）
    /// </summary>
    Task<Dictionary<string, List<TEntity>>> BatchQueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Dictionary<string, Expression<Func<TEntity, bool>>> predicatesMap);

    /// <summary>
    /// 同时获取多个指标（批处理优化）
    /// </summary>
    Task<Dictionary<string, decimal>> GetMultipleMetricsAsync<TEntity>(
        IEnumerable<string> filePaths,
        Dictionary<string, string> metricsMap,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 获取文件中的记录数
    /// </summary>
    Task<int> CountAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 获取特定列的合计值
    /// </summary>
    Task<decimal> SumAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 获取特定列的平均值
    /// </summary>
    Task<decimal> AvgAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 获取特定列的最小值
    /// </summary>
    Task<decimal> MinAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 获取特定列的最大值
    /// </summary>
    Task<decimal> MaxAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 通过自定义SQL查询数据
    /// </summary>
    Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, params object[] parameters);
}
