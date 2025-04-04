using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB Parquet文件查询提供程序接口
/// </summary>
public interface IDuckDBFileQueryProvider : IDuckDBProviderAdvanced, IDisposable
{
    /// <summary>
    /// 查询Parquet文件
    /// </summary>
    Task<List<TEntity>> QueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取符合条件的第一条记录
    /// </summary>
    Task<TEntity> FirstOrDefaultAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 分页查询Parquet文件
    /// </summary>
    Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 0,
        int pageSize = 50,
        Expression<Func<TEntity, object>> orderBy = null,
        bool ascending = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 流式查询大量数据，避免一次性加载所有内容到内存
    /// </summary>
    Task<int> QueryStreamAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        Func<IEnumerable<TEntity>, Task> processAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 以异步流方式查询数据
    /// </summary>
    IAsyncEnumerable<TEntity> QueryStreamEnumerableAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default);

    #region 聚合方法

    /// <summary>
    /// 执行自定义分组查询
    /// </summary>
    Task<List<TResult>> ExecuteGroupByQueryAsync<TResult>(
        IEnumerable<string> filePaths,
        string selectClause,
        string whereClause = null,
        string groupByClause = null,
        string orderByClause = null,
        Dictionary<string, object> parameters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计算聚合值 - 通用聚合方法
    /// </summary>
    Task<TResult> AggregateAsync<TEntity, TResult>(
        string aggregateFunction,
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计算列总和
    /// </summary>
    Task<TResult> SumAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计算指定列的总和（基于列名）
    /// </summary>
    Task<TResult> SumAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        string columnName,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计算列平均值
    /// </summary>
    Task<TResult> AvgAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计算列最小值
    /// </summary>
    Task<TResult> MinAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 计算列最大值
    /// </summary>
    Task<TResult> MaxAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 统计满足条件的记录数，返回指定类型
    /// </summary>
    Task<TResult> CountAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default);

    #endregion

    #region 分组聚合方法

    /// <summary>
    /// 按指定字段分组计数
    /// </summary>
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
    Task<List<(TGroupKey Key, TResult Sum)>> SumByAsync<TEntity, TGroupKey, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> sumExpression,
        Expression<Func<TEntity, TGroupKey>> groupExpression,
        Expression<Func<TEntity, bool>> predicate = null,
        string orderByColumn = null,
        bool ascending = true,
        CancellationToken cancellationToken = default);

    #endregion
}
