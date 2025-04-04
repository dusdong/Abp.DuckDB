using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Castle.Core.Logging;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// 提供用于查询Parquet文件的特定功能的DuckDB提供程序
/// </summary>
public class DuckDBFileQueryProvider : DuckDbProviderAdvanced, IDuckDBFileQueryProvider
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="logger">日志记录器</param>
    public DuckDBFileQueryProvider(ILogger logger, DuckDBSqlBuilder sqlBuilder, QueryPerformanceMonitor performanceMonitor)
        : base(logger, sqlBuilder, performanceMonitor)
    {
        // 基类已经初始化了必要组件
    }

    #region 核心查询方法

    /// <summary>
    /// 查询Parquet文件
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="filePaths">Parquet文件路径集合</param>
    /// <param name="predicate">过滤条件表达式</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>满足条件的实体列表</returns>
    public async Task<List<TEntity>> QueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);
        int fileCount = filePaths.Count();

        return await ExecuteSafelyAsync(
            async () =>
            {
                // 构建查询参数
                var whereClause = predicate != null
                    ? _sqlBuilder.BuildWhereClause(predicate)
                    : string.Empty;

                var selectedColumns = GetEntityColumns<TEntity>();
                var dataSource = BuildDataSource(filePaths);

                // 构建并执行SQL
                var sql = BuildSelectQuery(dataSource, whereClause, selectedColumns);

                return await ExecuteQueryWithMetricsAsync(
                    () => QueryWithRawSqlAsync<TEntity>(sql, cancellationToken),
                    "QueryParquet",
                    sql,
                    fileCount);
            },
            "查询Parquet文件",
            $"文件数: {fileCount}"
        );
    }

    /// <summary>
    /// 获取符合条件的第一条记录
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="filePaths">Parquet文件路径集合</param>
    /// <param name="predicate">过滤条件表达式</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>满足条件的实体，如果没有则返回default</returns>
    public async Task<TEntity> FirstOrDefaultAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);
        int fileCount = filePaths.Count();

        return await ExecuteSafelyAsync(
            async () =>
            {
                // 构建查询参数
                var whereClause = predicate != null
                    ? _sqlBuilder.BuildWhereClause(predicate)
                    : string.Empty;

                var selectedColumns = GetEntityColumns<TEntity>();
                var dataSource = BuildDataSource(filePaths);

                // 构建查询并添加LIMIT 1来只获取第一条记录
                var sql = BuildSelectQuery(dataSource, whereClause, selectedColumns) + " LIMIT 1";

                var results = await ExecuteQueryWithMetricsAsync(
                    () => QueryWithRawSqlAsync<TEntity>(sql, cancellationToken),
                    "FirstOrDefaultParquet",
                    sql,
                    fileCount);

                return results.FirstOrDefault();
            },
            "获取第一条Parquet记录",
            $"文件数: {fileCount}"
        );
    }

    /// <summary>
    /// 分页查询Parquet文件
    /// </summary>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 0,
        int pageSize = 50,
        Expression<Func<TEntity, object>> orderBy = null,
        bool ascending = true,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);
        int fileCount = filePaths.Count();

        return await ExecuteSafelyAsync(
            async () =>
            {
                string orderByColumn = orderBy != null ? GetColumnName(orderBy) : null;
                string whereClause = predicate != null ? _sqlBuilder.BuildWhereClause(predicate) : string.Empty;
                string dataSource = BuildDataSource(filePaths);
                var selectedColumns = GetEntityColumns<TEntity>();

                return await ExecutePagedQueryAsync<TEntity>(
                    dataSource,
                    whereClause,
                    pageIndex,
                    pageSize,
                    orderByColumn,
                    ascending,
                    selectedColumns,
                    cancellationToken);
            },
            "分页查询Parquet文件",
            $"文件数: {fileCount}, 页码: {pageIndex}, 每页大小: {pageSize}"
        );
    }

    /// <summary>
    /// 流式查询大量数据，避免一次性加载所有内容到内存
    /// </summary>
    public async Task<int> QueryStreamAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        Func<IEnumerable<TEntity>, Task> processAction = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);
        int fileCount = filePaths.Count();

        return await ExecuteSafelyAsync(
            async () =>
            {
                var dataSource = BuildDataSource(filePaths);
                var whereClause = predicate != null
                    ? _sqlBuilder.BuildWhereClause(predicate)
                    : string.Empty;

                return await QueryStreamInternalAsync<TEntity>(
                    dataSource,
                    whereClause,
                    batchSize,
                    processAction,
                    cancellationToken);
            },
            "流式查询Parquet文件",
            $"文件数: {fileCount}, 批大小: {batchSize}"
        );
    }

    /// <summary>
    /// 以异步流方式查询数据
    /// </summary>
    public async IAsyncEnumerable<TEntity> QueryStreamEnumerableAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        [EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        // 构建查询参数但不放在try块内
        var whereClause = predicate != null
            ? _sqlBuilder.BuildWhereClause(predicate)
            : string.Empty;

        var dataSource = BuildDataSource(filePaths);
        var sql = BuildSelectQuery(dataSource, whereClause);

        using var command = _connection.CreateCommand();
        command.CommandText = sql;

        if (_configuration.CommandTimeout != TimeSpan.Zero)
            command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var properties = GetEntityProperties<TEntity>();
        var columnMappings = GetColumnMappings(reader, properties);
        int itemCount = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            TEntity entity;
            try
            {
                entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
            }
            catch (Exception ex)
            {
                _logger.Error($"映射实体失败: {ex.Message}", ex);
                continue; // 跳过这一项，继续下一项
            }

            itemCount++;

            // 可选：按批次大小记录性能统计
            if (itemCount % batchSize == 0 && _configuration.EnablePerformanceMonitoring)
            {
                RecordBatchProcessing(itemCount);
            }

            yield return entity;
        }
    }

    #endregion

    #region 聚合方法 - 合并重复的聚合方法实现

    /// <summary>
    /// 执行自定义分组查询
    /// </summary>
    public async Task<List<TResult>> ExecuteGroupByQueryAsync<TResult>(
        IEnumerable<string> filePaths,
        string selectClause,
        string whereClause = null,
        string groupByClause = null,
        string orderByClause = null,
        Dictionary<string, object> parameters = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        try
        {
            string dataSource = BuildDataSource(filePaths);

            // 构建SQL
            var sql = new StringBuilder($"SELECT {selectClause} FROM {dataSource}");

            if (!string.IsNullOrEmpty(whereClause))
                sql.Append($" WHERE {whereClause}");

            if (!string.IsNullOrEmpty(groupByClause))
                sql.Append($" GROUP BY {groupByClause}");

            if (!string.IsNullOrEmpty(orderByClause))
                sql.Append($" ORDER BY {orderByClause}");

            // 创建命令
            using var command = _connection.CreateCommand();
            command.CommandText = sql.ToString();

            // 添加参数
            if (parameters != null && parameters.Count > 0)
            {
                foreach (var param in parameters)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = param.Key;
                    parameter.Value = param.Value ?? DBNull.Value;
                    command.Parameters.Add(parameter);
                }
            }

            if (_configuration.CommandTimeout != TimeSpan.Zero)
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

            // 执行查询
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var results = new List<TResult>();

            var type = typeof(TResult);
            var isSimpleType = type.IsPrimitive || type == typeof(string) || type == typeof(decimal) ||
                               type == typeof(DateTime) || Nullable.GetUnderlyingType(type) != null;

            while (await reader.ReadAsync(cancellationToken))
            {
                if (isSimpleType)
                {
                    var value = reader.GetValue(0);
                    results.Add((TResult)Convert.ChangeType(value, type));
                }
                else
                {
                    var properties = GetEntityProperties<TResult>();
                    var columnMappings = GetColumnMappings(reader, properties);
                    var entity = MapReaderToEntity<TResult>(reader, properties, columnMappings);
                    results.Add(entity);
                }
            }

            return results;
        }
        catch (Exception ex)
        {
            _logger.Error($"执行分组查询失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 计算聚合值 - 通用聚合方法
    /// </summary>
    public async Task<TResult> AggregateAsync<TEntity, TResult>(
        string aggregateFunction,
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);
        int fileCount = filePaths.Count();

        return await ExecuteSafelyAsync(
            async () =>
            {
                string columnName = GetColumnName(selector);
                string whereClause = predicate != null ? _sqlBuilder.BuildWhereClause(predicate) : string.Empty;
                string dataSource = BuildDataSource(filePaths);

                // 验证聚合函数名称
                string upperFunction = aggregateFunction.ToUpperInvariant();
                if (!IsValidAggregateFunction(upperFunction))
                {
                    throw new ArgumentException($"不支持的聚合函数: {aggregateFunction}", nameof(aggregateFunction));
                }

                return await ExecuteAggregateAsync<TEntity, TResult>(
                    upperFunction,
                    columnName,
                    whereClause,
                    dataSource,
                    cancellationToken);
            },
            $"执行{aggregateFunction}聚合",
            $"文件数: {fileCount}"
        );
    }

    /// <summary>
    /// 验证聚合函数名称
    /// </summary>
    private bool IsValidAggregateFunction(string function)
    {
        return function == "SUM" || function == "AVG" || function == "MIN" || function == "MAX" ||
               function == "COUNT" || function == "FIRST" || function == "LAST";
    }

    /// <summary>
    /// 计算列总和
    /// </summary>
    public async Task<TResult> SumAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        return await AggregateAsync<TEntity, TResult>(
            "SUM",
            filePaths,
            selector,
            predicate,
            cancellationToken);
    }

    /// <summary>
    /// 计算列平均值
    /// </summary>
    public async Task<TResult> AvgAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        return await AggregateAsync<TEntity, TResult>(
            "AVG",
            filePaths,
            selector,
            predicate,
            cancellationToken);
    }

    /// <summary>
    /// 计算列最小值
    /// </summary>
    public async Task<TResult> MinAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        return await AggregateAsync<TEntity, TResult>(
            "MIN",
            filePaths,
            selector,
            predicate,
            cancellationToken);
    }

    /// <summary>
    /// 计算列最大值
    /// </summary>
    public async Task<TResult> MaxAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        return await AggregateAsync<TEntity, TResult>(
            "MAX",
            filePaths,
            selector,
            predicate,
            cancellationToken);
    }

    /// <summary>
    /// 统计满足条件的记录数，返回指定类型
    /// </summary>
    public async Task<TResult> CountAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);
        int fileCount = filePaths.Count();

        return await ExecuteSafelyAsync(
            async () =>
            {
                var whereClause = predicate != null
                    ? _sqlBuilder.BuildWhereClause(predicate)
                    : string.Empty;

                var dataSource = BuildDataSource(filePaths);
                var sql = BuildCountQuery(dataSource, whereClause);

                return await ExecuteQueryWithMetricsAsync(
                    async () =>
                    {
                        using var command = _connection.CreateCommand();
                        command.CommandText = sql;

                        if (_configuration.CommandTimeout != TimeSpan.Zero)
                            command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

                        var result = await command.ExecuteScalarAsync(cancellationToken);
                        return DuckDBTypeConverter.SafeConvert<TResult>(result, _logger);
                    },
                    "CountParquet",
                    sql,
                    fileCount);
            },
            "计数Parquet记录",
            $"文件数: {fileCount}"
        );
    }

    #endregion

    #region 分组聚合方法

    /// <summary>
    /// 按字段分组计数
    /// </summary>
    public async Task<List<(TGroupKey Key, TResult Count)>> CountByAsync<TEntity, TGroupKey, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TGroupKey>> groupExpression,
        Expression<Func<TEntity, bool>> predicate = null,
        string orderByColumn = null,
        bool ascending = true,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        try
        {
            string groupColumn = GetColumnName(groupExpression);
            string whereClause = predicate != null ? _sqlBuilder.BuildWhereClause(predicate) : string.Empty;
            string dataSource = BuildDataSource(filePaths);

            // 构建分组SQL
            string sql = $"SELECT {groupColumn}, COUNT(*) as Count FROM {dataSource}";

            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            sql += $" GROUP BY {groupColumn}";

            if (!string.IsNullOrEmpty(orderByColumn))
                sql += $" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}";
            else
                sql += $" ORDER BY Count {(ascending ? "ASC" : "DESC")}";

            // 执行查询
            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            if (_configuration.CommandTimeout != TimeSpan.Zero)
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var result = new List<(TGroupKey Key, TResult Count)>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var key = (TGroupKey)Convert.ChangeType(reader.GetValue(0), typeof(TGroupKey));
                var count = (TResult)Convert.ChangeType(reader.GetValue(1), typeof(TResult));

                result.Add((key, count));
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.Error($"分组计数失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 按字段分组求和
    /// </summary>
    public async Task<List<(TGroupKey Key, TResult Sum)>> SumByAsync<TEntity, TGroupKey, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> sumExpression,
        Expression<Func<TEntity, TGroupKey>> groupExpression,
        Expression<Func<TEntity, bool>> predicate = null,
        string orderByColumn = null,
        bool ascending = true,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        try
        {
            string sumColumn = GetColumnName(sumExpression);
            string groupColumn = GetColumnName(groupExpression);
            string whereClause = predicate != null ? _sqlBuilder.BuildWhereClause(predicate) : string.Empty;
            string dataSource = BuildDataSource(filePaths);

            // 构建分组求和SQL
            string sql = $"SELECT {groupColumn}, SUM({sumColumn}) as Total FROM {dataSource}";

            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            sql += $" GROUP BY {groupColumn}";

            if (!string.IsNullOrEmpty(orderByColumn))
                sql += $" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}";
            else
                sql += $" ORDER BY Total {(ascending ? "ASC" : "DESC")}";

            // 执行查询
            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            if (_configuration.CommandTimeout != TimeSpan.Zero)
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var result = new List<(TGroupKey Key, TResult Sum)>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var key = (TGroupKey)Convert.ChangeType(reader.GetValue(0), typeof(TGroupKey));
                var sum = (TResult)Convert.ChangeType(reader.GetValue(1), typeof(TResult));

                result.Add((key, sum));
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.Error($"分组求和失败: {ex.Message}", ex);
            throw;
        }
    }

    #endregion

    #region 辅助方法

    /// <summary>
    /// 构建文件数据源
    /// </summary>
    protected string BuildDataSource(IEnumerable<string> filePaths)
    {
        // 使用优化后的文件路径处理
        return _sqlBuilder.BuildParquetSourceClause(filePaths);
    }

    /// <summary>
    /// 验证文件路径集合
    /// </summary>
    /// <param name="filePaths">文件路径集合</param>
    /// <exception cref="ArgumentException">文件路径为空或无效</exception>
    protected void ValidateFilePaths(IEnumerable<string> filePaths)
    {
        if (filePaths == null || !filePaths.Any())
        {
            throw new ArgumentException("必须提供至少一个文件路径", nameof(filePaths));
        }

        // 优化：仅验证路径，不直接检查文件存在（可能耗时）
        foreach (var path in filePaths)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("文件路径不能为空", nameof(filePaths));
            }

            // 仅在日志级别为Debug时才验证文件存在
            if (_logger.IsDebugEnabled && !File.Exists(path))
            {
                _logger.Debug($"文件不存在可能导致查询结果为空: {path}");
            }
        }
    }

    #endregion
}
