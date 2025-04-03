using System.Linq.Expressions;
using DuckDB.NET.Data;

namespace Abp.DuckDB;

/// <summary>
/// DuckDB 查询提供程序高级接口
/// </summary>
public interface IDuckDBProviderAdvanced : IDuckDBProvider
{
    /// <summary>
    /// 获取原始DuckDB连接以执行高级操作
    /// </summary>
    DuckDBConnection GetDuckDBConnection();

    /// <summary>
    /// 使用向量化批量过滤执行查询
    /// </summary>
    Task<List<TEntity>> QueryWithVectorizedFiltersAsync<TEntity>(
        string tableName,
        string[] columns,
        object[][] filterValues,
        int resultLimit = 1000);

    /// <summary>
    /// 注册C#自定义函数到DuckDB
    /// </summary>
    void RegisterFunction<TReturn, TParam1>(string functionName, Func<TParam1, TReturn> function);

    /// <summary>
    /// 执行非查询SQL语句
    /// </summary>
    Task<int> ExecuteNonQueryAsync(string sql, params object[] parameters);

    /// <summary>
    /// 直接从Parquet文件执行查询
    /// </summary>
    Task<List<TEntity>> QueryParquetFileAsync<TEntity>(
        string parquetFilePath,
        Expression<Func<TEntity, bool>> predicate = null);

    /// <summary>
    /// 应用优化设置
    /// </summary>
    Task ApplyOptimizationAsync();

    /// <summary>
    /// 带有限制和偏移的分页查询
    /// </summary>
    Task<List<TEntity>> QueryWithLimitOffsetAsync<TEntity>(
        Expression<Func<TEntity, bool>> predicate = null,
        int limit = 1000,
        int offset = 0,
        string orderByColumn = null,
        bool ascending = true);
}
