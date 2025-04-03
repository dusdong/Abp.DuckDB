using System.Linq.Expressions;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB文件查询提供程序接口
/// </summary>
public interface IDuckDBFileQueryProvider : IDuckDBProviderAdvanced
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

    /// <summary>
    /// 执行分组聚合查询
    /// </summary>
    /// <typeparam name="TResult">结果类型</typeparam>
    /// <param name="files">要查询的文件列表</param>
    /// <param name="selectClause">SELECT子句（不包含SELECT关键字）</param>
    /// <param name="whereClause">WHERE子句（不包含WHERE关键字），可选</param>
    /// <param name="groupByClause">GROUP BY子句（不包含GROUP BY关键字），可选</param>
    /// <param name="orderByClause">ORDER BY子句（不包含ORDER BY关键字），可选</param>
    /// <param name="parameters">查询参数，可选</param>
    /// <param name="cancellationToken">取消令牌，可选</param>
    Task<List<TResult>> ExecuteGroupByQueryAsync<TResult>(
        IEnumerable<string> files,
        string selectClause,
        string whereClause = null,
        string groupByClause = null,
        string orderByClause = null,
        Dictionary<string, object> parameters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 按指定字段分组计数
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <typeparam name="TGroupKey">分组键类型</typeparam>
    /// <typeparam name="TResult">计数结果类型，通常为 int 或 long</typeparam>
    /// <param name="filePaths">要查询的文件列表</param>
    /// <param name="groupExpression">分组表达式，如 p => p.CategoryId</param>
    /// <param name="predicate">筛选条件，可选</param>
    /// <param name="orderByColumn">排序列，可选</param>
    /// <param name="ascending">是否升序，默认为true</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>按分组键和计数结果的元组列表</returns>
    Task<List<(TGroupKey Key, TResult Count)>> CountByAsync<TEntity, TGroupKey, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TGroupKey>> groupExpression,
        Expression<Func<TEntity, bool>> predicate = null,
        string orderByColumn = null,
        bool ascending = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 按指定字段分组求和
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <typeparam name="TGroupKey">分组键类型</typeparam>
    /// <typeparam name="TResult">求和结果类型</typeparam>
    /// <param name="filePaths">要查询的文件列表</param>
    /// <param name="sumExpression">求和表达式，如 p => p.Amount</param>
    /// <param name="groupExpression">分组表达式，如 p => p.CategoryId</param>
    /// <param name="predicate">筛选条件，可选</param>
    /// <param name="orderByColumn">排序列，可选</param>
    /// <param name="ascending">是否升序，默认为true</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>按分组键和求和结果的元组列表</returns>
    Task<List<(TGroupKey Key, TResult Sum)>> SumByAsync<TEntity, TGroupKey, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> sumExpression,
        Expression<Func<TEntity, TGroupKey>> groupExpression,
        Expression<Func<TEntity, bool>> predicate = null,
        string orderByColumn = null,
        bool ascending = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计数并返回指定类型结果
    /// </summary>
    Task<TResult> CountAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 求和并返回指定类型结果
    /// </summary>
    Task<TResult> SumAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// 求最小值并返回指定类型结果
    /// </summary>
    Task<TResult> MinAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 求最大值并返回指定类型结果
    /// </summary>
    Task<TResult> MaxAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);
}
