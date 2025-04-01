using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Abp.Dependency;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// 基于DuckDB的文件查询提供程序实现
/// </summary>
public class DuckDBFileQueryProvider : IDuckDBFileQueryProvider, ITransientDependency, IDisposable
{
    private DuckDBConnection _connection;
    private readonly ILogger _logger;
    private bool _disposed = false;

    public DuckDBFileQueryProvider(ILogger logger)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// 初始化DuckDB查询提供程序
    /// </summary>
    public void Initialize(string connectionString)
    {
        try
        {
            // 使用内存模式
            _connection = new DuckDBConnection("Data Source=:memory:");
            _connection.Open();

            // 设置并行线程数（根据可用处理器核心数）
            int processorCount = Environment.ProcessorCount;
            int threads = Math.Max(2, Math.Min(processorCount - 1, 8)); // 使用2-8个线程

            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = $"PRAGMA threads={threads};";
                cmd.ExecuteNonQuery();
            }

            _logger.Info($"DuckDB查询提供程序初始化成功，线程数：{threads}");
        }
        catch (Exception ex)
        {
            _logger.Error("初始化DuckDB查询提供程序失败", ex);
            throw;
        }
    }

    /// <summary>
    /// 查询特定类型的存档数据
    /// </summary>
    public async Task<List<TEntity>> QueryAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        try
        {
            // 获取实体类型的属性信息，用于列投影（使用缓存）
            var selectedColumns = GetEntityColumns<TEntity>();

            // 直接查询文件，不使用参数化查询，添加列投影
            var sql = BuildDirectParquetQuery<TEntity>(filePaths, predicate, selectedColumns);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;
                    return await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
                },
                "Query",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"查询DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 流式查询特定类型的存档数据，适用于大结果集
    /// </summary>
    public async Task<int> QueryStreamAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        Func<IEnumerable<TEntity>, Task> processAction = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        if (batchSize <= 0)
            throw new ArgumentException("批处理大小必须大于0", nameof(batchSize));

        try
        {
            // 获取实体类型的属性信息，用于列投影（使用缓存）
            var selectedColumns = GetEntityColumns<TEntity>();

            // 直接查询文件，不使用参数化查询，添加列投影
            var sql = BuildDirectParquetQuery<TEntity>(filePaths, predicate, selectedColumns);

            var stopwatch = Stopwatch.StartNew();
            int totalProcessed = 0;

            using var command = _connection.CreateCommand();
            command.CommandText = sql;
            // 开启流式模式
            command.UseStreamingMode = true;

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            // 从缓存获取属性（优化）
            var type = typeof(TEntity);
            var properties = DuckDBMetadataCache.GetOrAddProperties(type, t =>
                t.GetProperties()
                    .Where(prop => !prop.GetCustomAttributes(typeof(NotMappedAttribute), true).Any())
                    .ToArray());

            // 缓存列索引以提高性能
            var columnMappings = BuildColumnMappings(reader, properties);

            var batch = new List<TEntity>(batchSize);

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
                batch.Add(entity);
                totalProcessed++;

                // 当达到批处理大小时，处理当前批次
                if (batch.Count >= batchSize)
                {
                    if (processAction != null)
                    {
                        await processAction(batch).ConfigureAwait(false);
                    }

                    batch.Clear();
                }
            }

            // 处理剩余的批次
            if (batch.Count > 0 && processAction != null)
            {
                await processAction(batch).ConfigureAwait(false);
            }

            stopwatch.Stop();

            _logger.Info($"[DuckDB流式处理] 文件数: {filePaths.Count()} | " +
                         $"总记录数: {totalProcessed} | 批大小: {batchSize} | " +
                         $"执行时间: {stopwatch.ElapsedMilliseconds}ms");

            _logger.Debug($"[DuckDB流式SQL] {sql}");

            return totalProcessed;
        }
        catch (Exception ex)
        {
            _logger.Error($"流式查询DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 流式查询特定类型的存档数据，返回一个异步枚举器，适用于大结果集
    /// </summary>
    public async IAsyncEnumerable<TEntity> QueryStreamEnumerableAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        if (batchSize <= 0)
            throw new ArgumentException("批处理大小必须大于0", nameof(batchSize));

        // 获取实体类型的属性信息，用于列投影（使用缓存）
        var selectedColumns = GetEntityColumns<TEntity>();

        // 直接查询文件，不使用参数化查询，添加列投影
        var sql = BuildDirectParquetQuery<TEntity>(filePaths, predicate, selectedColumns);

        var stopwatch = Stopwatch.StartNew();
        int processedCount = 0;

        try
        {
            using var command = _connection.CreateCommand();
            command.CommandText = sql;
            // 开启流式模式
            command.UseStreamingMode = true;

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            // 从缓存获取属性（优化）
            var type = typeof(TEntity);
            var properties = DuckDBMetadataCache.GetOrAddProperties(type, t =>
                t.GetProperties()
                    .Where(prop => !prop.GetCustomAttributes(typeof(NotMappedAttribute), true).Any())
                    .ToArray());

            // 缓存列索引以提高性能
            var columnMappings = BuildColumnMappings(reader, properties);

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
                processedCount++;

                yield return entity;

                // 每读取 batchSize 条记录记录一次指标
                if (processedCount % batchSize == 0)
                {
                    _logger.Debug($"[DuckDB流式进度] 已处理记录: {processedCount}");
                }
            }
        }
        finally
        {
            stopwatch.Stop();
            _logger.Info($"[DuckDB流式枚举] 文件数: {filePaths.Count()} | " +
                         $"总记录数: {processedCount} | " +
                         $"执行时间: {stopwatch.ElapsedMilliseconds}ms");

            _logger.Debug($"[DuckDB流式SQL] {sql}");
        }
    }

    /// <summary>
    /// 批量查询多个不同条件的结果（批处理优化）
    /// </summary>
    public async Task<Dictionary<string, List<TEntity>>> BatchQueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Dictionary<string, Expression<Func<TEntity, bool>>> predicatesMap)
    {
        ValidateFilePaths(filePaths);

        if (predicatesMap == null || !predicatesMap.Any())
            throw new ArgumentException("必须提供至少一个查询条件", nameof(predicatesMap));

        var results = new Dictionary<string, List<TEntity>>();
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 获取实体类型的属性信息，用于列投影（使用缓存）
            var selectedColumns = GetEntityColumns<TEntity>();

            // 构建所有查询的组合脚本
            var queryScripts = new List<string>();
            foreach (var entry in predicatesMap)
            {
                string whereClause = entry.Value != null ? TranslateExpressionToSql(entry.Value) : "";
                string querySql = BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);
                queryScripts.Add(querySql);
            }

            // 批量执行所有查询
            var queryResults = await ExecuteBatchQueriesAsync<TEntity>(queryScripts);

            // 将结果映射回对应的键
            int index = 0;
            foreach (var key in predicatesMap.Keys)
            {
                if (index < queryResults.Count)
                    results[key] = queryResults[index];
                else
                    results[key] = new List<TEntity>();
                index++;
            }

            stopwatch.Stop();
            _logger.Info($"[DuckDB批处理指标] 批量查询 | 文件数: {filePaths.Count()} | " +
                         $"查询数: {predicatesMap.Count} | 执行时间: {stopwatch.ElapsedMilliseconds}ms");

            return results;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.Error($"批量查询DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 批量执行多个查询，减少多次连接开销
    /// </summary>
    private async Task<List<List<TEntity>>> ExecuteBatchQueriesAsync<TEntity>(List<string> sqlQueries)
    {
        var results = new List<List<TEntity>>();
        if (!sqlQueries.Any()) return results;

        // 使用事务来优化批处理
        using var transaction = _connection.BeginTransaction();
        try
        {
            foreach (var sql in sqlQueries)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = sql;
                command.Transaction = transaction;

                var queryResult = await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
                results.Add(queryResult);
            }

            transaction.Commit();
            return results;
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            _logger.Error($"批量执行查询失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 同时获取多个指标（批处理优化）
    /// </summary>
    public async Task<Dictionary<string, decimal>> GetMultipleMetricsAsync<TEntity>(
        IEnumerable<string> filePaths,
        Dictionary<string, string> metricsMap,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (metricsMap == null || !metricsMap.Any())
            throw new ArgumentException("必须提供至少一个指标", nameof(metricsMap));

        string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 构建单个SQL查询，同时计算多个指标
            var aggregations = string.Join(", ",
                metricsMap.Select(m => $"{m.Value} AS {m.Key}"));

            string sql;

            // 检查是否支持数组语法
            if (filePaths.Count() > 1 && filePaths.Count() <= 10)
            {
                var escapedPaths = filePaths.Select(p => $"'{p.Replace("'", "''")}'");
                string pathArray = $"[{string.Join(", ", escapedPaths)}]";

                sql = $"SELECT {aggregations} FROM read_parquet({pathArray})";
                if (!string.IsNullOrEmpty(whereClause))
                    sql += $" WHERE {whereClause}";
            }
            else if (filePaths.Count() == 1)
            {
                // 单文件查询
                string escapedPath = filePaths.First().Replace("'", "''");
                sql = $"SELECT {aggregations} FROM read_parquet('{escapedPath}')";

                if (!string.IsNullOrEmpty(whereClause))
                    sql += $" WHERE {whereClause}";
            }
            else
            {
                // 多文件先合并再计算
                var baseQuery = BuildDirectParquetQuery(filePaths, whereClause);
                sql = $"SELECT {aggregations} FROM ({baseQuery}) AS combined_data";
            }

            // 执行查询
            var result = new Dictionary<string, decimal>();

            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            if (await reader.ReadAsync().ConfigureAwait(false))
            {
                foreach (var key in metricsMap.Keys)
                {
                    var value = reader[key];
                    result[key] = (value == DBNull.Value) ? 0 : Convert.ToDecimal(value);
                }
            }

            stopwatch.Stop();
            _logger.Info($"[DuckDB批处理指标] 多指标查询 | 文件数: {filePaths.Count()} | " +
                         $"指标数: {metricsMap.Count} | 执行时间: {stopwatch.ElapsedMilliseconds}ms");

            _logger.Debug($"[DuckDB指标SQL] {sql}");

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.Error($"获取多个指标失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 分页查询存档数据
    /// </summary>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = null,
        bool ascending = true)
    {
        ValidateFilePaths(filePaths);

        if (pageIndex < 1)
            throw new ArgumentException("页码必须大于或等于1", nameof(pageIndex));
        if (pageSize < 1)
            throw new ArgumentException("每页记录数必须大于或等于1", nameof(pageSize));

        try
        {
            string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";

            // 使用批处理同时获取总数和分页数据
            var queries = new List<string>();

            // 1. 总数查询
            string countSql = BuildDirectParquetCountQuery(filePaths, whereClause);
            queries.Add(countSql);

            // 2. 分页数据查询
            var selectedColumns = GetEntityColumns<TEntity>();
            string pagingSql = BuildDirectParquetPagedQuery<TEntity>(
                filePaths,
                whereClause,
                selectedColumns,
                orderByColumn,
                ascending,
                pageSize,
                (pageIndex - 1) * pageSize);
            queries.Add(pagingSql);

            var stopwatch = Stopwatch.StartNew();

            // 使用事务批处理执行查询
            int totalCount = 0;
            List<TEntity> items = new List<TEntity>();

            using (var transaction = _connection.BeginTransaction())
            {
                // 执行计数查询
                using (var countCommand = _connection.CreateCommand())
                {
                    countCommand.CommandText = countSql;
                    countCommand.Transaction = transaction;
                    totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync());
                }

                if (totalCount > 0)
                {
                    // 执行分页查询
                    using (var pageCommand = _connection.CreateCommand())
                    {
                        pageCommand.CommandText = pagingSql;
                        pageCommand.Transaction = transaction;
                        items = await ExecuteReaderAsync<TEntity>(pageCommand);
                    }
                }

                transaction.Commit();
            }

            stopwatch.Stop();
            _logger.Info($"[DuckDB批处理指标] 分页查询 | 文件数: {filePaths.Count()} | " +
                         $"总数: {totalCount} | 页数据: {items.Count} | 执行时间: {stopwatch.ElapsedMilliseconds}ms");

            return (items, totalCount);
        }
        catch (Exception ex)
        {
            _logger.Error($"分页查询DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 获取文件中的记录数
    /// </summary>
    public async Task<int> CountAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        try
        {
            string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";
            string countSql = BuildDirectParquetCountQuery(filePaths, whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = countSql;
                    return Convert.ToInt32(await command.ExecuteScalarAsync());
                },
                "Count",
                countSql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"统计DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 获取特定列的合计值
    /// </summary>
    public async Task<decimal> SumAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求和的属性名
            string columnName = GetColumnName(selector);
            string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";

            // 构建直接查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"SUM({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;
                    var result = await command.ExecuteScalarAsync();

                    if (result == null || result == DBNull.Value)
                        return 0;

                    return Convert.ToDecimal(result);
                },
                "Sum",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"求和DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 获取特定列的平均值
    /// </summary>
    public async Task<decimal> AvgAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求平均值的属性名
            string columnName = GetColumnName(selector);
            string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";

            // 构建直接查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"AVG({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;
                    var result = await command.ExecuteScalarAsync();

                    if (result == null || result == DBNull.Value)
                        return 0;

                    return Convert.ToDecimal(result);
                },
                "Avg",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"求平均值DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 获取特定列的最小值
    /// </summary>
    public async Task<decimal> MinAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求最小值的属性名
            string columnName = GetColumnName(selector);
            string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";

            // 构建直接查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"MIN({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;
                    var result = await command.ExecuteScalarAsync();

                    if (result == null || result == DBNull.Value)
                        return 0;

                    return Convert.ToDecimal(result);
                },
                "Min",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"求最小值DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 获取特定列的最大值
    /// </summary>
    public async Task<decimal> MaxAsync<TEntity>(IEnumerable<string> filePaths, Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求最大值的属性名
            string columnName = GetColumnName(selector);
            string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";

            // 构建直接查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"MAX({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;
                    var result = await command.ExecuteScalarAsync();

                    if (result == null || result == DBNull.Value)
                        return 0;

                    return Convert.ToDecimal(result);
                },
                "Max",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"求最大值DuckDB数据失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 通过自定义SQL查询数据
    /// </summary>
    public async Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, params object[] parameters)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentNullException(nameof(sql));

        try
        {
            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    if (parameters != null && parameters.Length > 0)
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            var parameter = new DuckDBParameter
                            {
                                ParameterName = $"p{i}",
                                Value = parameters[i] ?? DBNull.Value
                            };
                            command.Parameters.Add(parameter);
                        }
                    }

                    return await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
                },
                "RawSql",
                sql,
                0);
        }
        catch (Exception ex)
        {
            _logger.Error($"执行自定义SQL查询失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 带指标收集的查询执行方法
    /// </summary>
    private async Task<T> ExecuteQueryWithMetricsAsync<T>(
        Func<Task<T>> queryFunc,
        string queryType,
        string sql,
        int filesCount)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            T result = await queryFunc();
            stopwatch.Stop();

            // 计算结果集大小（如果可能）
            int resultCount = -1;
            if (result is ICollection<object> collection)
                resultCount = collection.Count;
            else if (result is int intValue)
                resultCount = 1;

            // 记录基本指标
            _logger.Info($"[DuckDB指标] 类型: {queryType} | 文件数: {filesCount} | " +
                         $"结果数: {(resultCount >= 0 ? resultCount.ToString() : "N/A")} | " +
                         $"执行时间: {stopwatch.ElapsedMilliseconds}ms");

            _logger.Debug($"[DuckDB指标SQL] {sql}");

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.Error($"[DuckDB指标错误] 类型: {queryType} | 文件数: {filesCount} | " +
                          $"执行时间: {stopwatch.ElapsedMilliseconds}ms | 错误: {ex.Message}");
            throw;
        }
    }

    #region Helper Methods

    /// <summary>
    /// 构建列映射字典（优化使用缓存）
    /// </summary>
    private Dictionary<PropertyInfo, int> BuildColumnMappings(
        System.Data.Common.DbDataReader reader,
        PropertyInfo[] properties)
    {
        // 构建读取器列名到索引的映射
        var columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnIndexMap[reader.GetName(i)] = i;
        }

        // 获取属性声明类型
        var type = properties.Length > 0 ? properties[0].DeclaringType : null;
        if (type == null) return new Dictionary<PropertyInfo, int>();

        // 从缓存获取属性到列名的映射
        var propertyColumnMap = DuckDBMetadataCache.GetOrAddPropertyColumnMappings(type, t =>
        {
            var map = new Dictionary<string, string>();
            foreach (var prop in properties)
            {
                var columnAttribute = prop.GetCustomAttributes(typeof(ColumnAttribute), true)
                    .FirstOrDefault() as ColumnAttribute;

                string columnName = columnAttribute?.Name ?? prop.Name;
                map[prop.Name] = columnName;
            }

            return map;
        });

        // 使用缓存的列名映射创建属性到索引的映射
        var columnMappings = new Dictionary<PropertyInfo, int>();
        foreach (var prop in properties)
        {
            if (propertyColumnMap.TryGetValue(prop.Name, out string columnName) &&
                columnIndexMap.TryGetValue(columnName, out int columnIndex))
            {
                columnMappings[prop] = columnIndex;
            }
        }

        return columnMappings;
    }

    /// <summary>
    /// 从数据读取器映射实体
    /// </summary>
    private TEntity MapReaderToEntity<TEntity>(
        System.Data.Common.DbDataReader reader,
        PropertyInfo[] properties,
        Dictionary<PropertyInfo, int> columnMappings)
    {
        var entity = Activator.CreateInstance<TEntity>();

        foreach (var mapping in columnMappings)
        {
            var prop = mapping.Key;
            var columnIndex = mapping.Value;

            if (reader.IsDBNull(columnIndex))
                continue;

            var value = reader.GetValue(columnIndex);

            try
            {
                // 处理类型转换
                if (prop.PropertyType.IsEnum && value is string strValue)
                {
                    // 字符串枚举值转换
                    prop.SetValue(entity, Enum.Parse(prop.PropertyType, strValue));
                }
                else if (prop.PropertyType == typeof(DateTime) && value is string dateStr)
                {
                    // 字符串日期转换
                    prop.SetValue(entity, DateTime.Parse(dateStr));
                }
                else if (prop.PropertyType == typeof(Guid) && value is string guidStr)
                {
                    // 字符串GUID转换
                    prop.SetValue(entity, Guid.Parse(guidStr));
                }
                else
                {
                    // 标准类型转换
                    prop.SetValue(entity, Convert.ChangeType(value,
                        Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType));
                }
            }
            catch (Exception ex)
            {
                _logger.Error(
                    $"属性{prop.Name}设值失败。值类型:{value?.GetType().Name ?? "null"}，" +
                    $"目标类型:{prop.PropertyType.Name}，值:{value}，错误:{ex.Message}", ex);
                // 继续处理下一个属性
            }
        }

        return entity;
    }

    /// <summary>
    /// 获取实体类型的列名（使用缓存）
    /// </summary>
    private IEnumerable<string> GetEntityColumns<TEntity>()
    {
        var type = typeof(TEntity);
        return DuckDBMetadataCache.GetOrAddEntityColumns(type, t =>
        {
            var properties = t.GetProperties()
                .Where(prop => !prop.GetCustomAttributes(typeof(NotMappedAttribute), true).Any());

            return properties.Select(p =>
            {
                var columnAttribute = p.GetCustomAttributes(typeof(ColumnAttribute), true)
                    .FirstOrDefault() as ColumnAttribute;
                return columnAttribute?.Name ?? p.Name;
            }).ToList();
        });
    }

    /// <summary>
    /// 直接构建查询SQL，不使用CTE，避免语法错误（使用缓存转换表达式）
    /// </summary>
    private string BuildDirectParquetQuery<TEntity>(IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        IEnumerable<string> selectedColumns = null)
    {
        string whereClause = predicate != null ? TranslateExpressionToSql(predicate) : "";
        return BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);
    }

    /// <summary>
    /// 构建直接的Parquet查询
    /// </summary>
    private string BuildDirectParquetQuery(
        IEnumerable<string> filePaths,
        string whereClause,
        IEnumerable<string> selectedColumns = null)
    {
        // 确定查询的列
        string columns = "*";
        if (selectedColumns != null && selectedColumns.Any())
        {
            columns = string.Join(", ", selectedColumns);
        }

        // 检查是否支持数组语法
        bool useArraySyntax = false;
        try
        {
            // 通过简单测试确定DuckDB版本是否支持数组语法
            if (filePaths.Count() > 1 && filePaths.Count() <= 10)
            {
                useArraySyntax = true;
            }
        }
        catch
        {
            // 如果出错，就不使用数组语法
            useArraySyntax = false;
        }

        if (useArraySyntax)
        {
            // 使用数组语法
            var escapedPaths = filePaths.Select(p => $"'{p.Replace("'", "''")}'");
            string pathArray = $"[{string.Join(", ", escapedPaths)}]";

            string sql = $"SELECT {columns} FROM read_parquet({pathArray})";
            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            return sql;
        }
        else if (filePaths.Count() == 1)
        {
            // 单文件查询，简化语法
            string escapedPath = filePaths.First().Replace("'", "''");
            string sql = $"SELECT {columns} FROM read_parquet('{escapedPath}')";

            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            return sql;
        }
        else
        {
            // 大量文件仍使用UNION ALL查询
            var queries = new List<string>();

            foreach (var filePath in filePaths)
            {
                string escapedPath = filePath.Replace("'", "''");
                string query = $"SELECT {columns} FROM read_parquet('{escapedPath}')";

                if (!string.IsNullOrEmpty(whereClause))
                    query += $" WHERE {whereClause}";

                queries.Add(query);
            }

            return string.Join(" UNION ALL ", queries);
        }
    }

    /// <summary>
    /// 构建直接的Parquet计数查询
    /// </summary>
    private string BuildDirectParquetCountQuery(IEnumerable<string> filePaths, string whereClause)
    {
        // 检查是否支持数组语法
        bool useArraySyntax = false;
        try
        {
            if (filePaths.Count() > 1 && filePaths.Count() <= 10)
            {
                useArraySyntax = true;
            }
        }
        catch
        {
            useArraySyntax = false;
        }

        if (useArraySyntax)
        {
            var escapedPaths = filePaths.Select(p => $"'{p.Replace("'", "''")}'");
            string pathArray = $"[{string.Join(", ", escapedPaths)}]";

            string sql = $"SELECT COUNT(*) FROM read_parquet({pathArray})";
            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            return sql;
        }
        else if (filePaths.Count() == 1)
        {
            // 单文件查询计数
            string escapedPath = filePaths.First().Replace("'", "''");
            string sql = $"SELECT COUNT(*) FROM read_parquet('{escapedPath}')";

            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            return sql;
        }
        else
        {
            // 多文件先合并再计数
            var baseQuery = BuildDirectParquetQuery(filePaths, whereClause);
            return $"SELECT COUNT(*) FROM ({baseQuery}) AS combined_data";
        }
    }

    /// <summary>
    /// 构建直接的Parquet聚合查询(SUM, AVG, MIN, MAX)
    /// </summary>
    private string BuildDirectParquetAggregateQuery(
        IEnumerable<string> filePaths,
        string aggregateFunction,
        string whereClause)
    {
        // 检查是否支持数组语法
        bool useArraySyntax = false;
        try
        {
            if (filePaths.Count() > 1 && filePaths.Count() <= 10)
            {
                useArraySyntax = true;
            }
        }
        catch
        {
            useArraySyntax = false;
        }

        if (useArraySyntax)
        {
            var escapedPaths = filePaths.Select(p => $"'{p.Replace("'", "''")}'");
            string pathArray = $"[{string.Join(", ", escapedPaths)}]";

            string sql = $"SELECT {aggregateFunction} FROM read_parquet({pathArray})";
            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            return sql;
        }
        else if (filePaths.Count() == 1)
        {
            // 单文件查询聚合
            string escapedPath = filePaths.First().Replace("'", "''");
            string sql = $"SELECT {aggregateFunction} FROM read_parquet('{escapedPath}')";

            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            return sql;
        }
        else
        {
            // 多文件先合并再聚合
            var baseQuery = BuildDirectParquetQuery(filePaths, whereClause);
            return $"SELECT {aggregateFunction} FROM ({baseQuery}) AS combined_data";
        }
    }

    /// <summary>
    /// 构建直接的Parquet分页查询
    /// </summary>
    private string BuildDirectParquetPagedQuery<TEntity>(
        IEnumerable<string> filePaths,
        string whereClause,
        IEnumerable<string> selectedColumns,
        string orderByColumn,
        bool ascending,
        int limit,
        int offset)
    {
        var baseQuery = BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);
        string sql = $"SELECT * FROM ({baseQuery}) AS combined_data";

        if (!string.IsNullOrEmpty(orderByColumn))
        {
            sql += $" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}";
        }

        sql += $" LIMIT {limit} OFFSET {offset}";
        return sql;
    }

    private void ValidateFilePaths(IEnumerable<string> filePaths)
    {
        if (filePaths == null || !filePaths.Any())
            throw new ArgumentException("必须提供至少一个文件路径", nameof(filePaths));
    }

    private string GetColumnName<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> selector)
    {
        if (selector.Body is MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }

        throw new ArgumentException("Selector必须是一个属性访问表达式");
    }

    /// <summary>
    /// 将表达式转换为SQL WHERE子句（使用缓存）
    /// </summary>
    private string TranslateExpressionToSql<TEntity>(Expression<Func<TEntity, bool>> predicate)
    {
        if (predicate == null)
            return string.Empty;

        try
        {
            string expressionKey = predicate.ToString();
            return DuckDBMetadataCache.GetOrAddExpressionSql(expressionKey, _ =>
            {
                var visitor = new SqlExpressionVisitor();
                string sqlCondition = visitor.Translate(predicate);

                _logger.Debug($"表达式 [{predicate}] 被翻译为 SQL: [{sqlCondition}]");
                return sqlCondition;
            });
        }
        catch (Exception ex)
        {
            _logger.Error($"无法将表达式转换为SQL: {ex.Message}", ex);
            throw new ArgumentException($"无法将表达式 [{predicate}] 转换为SQL", nameof(predicate), ex);
        }
    }

    /// <summary>
    /// 执行读取查询并返回实体列表（使用缓存）
    /// </summary>
    private async Task<List<TEntity>> ExecuteReaderAsync<TEntity>(DuckDBCommand command)
    {
        using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        var results = new List<TEntity>();

        // 从缓存获取属性（优化）
        var type = typeof(TEntity);
        var properties = DuckDBMetadataCache.GetOrAddProperties(type, t =>
            t.GetProperties()
                .Where(prop => !prop.GetCustomAttributes(typeof(NotMappedAttribute), true).Any())
                .ToArray());

        // 缓存列映射
        var columnMappings = BuildColumnMappings(reader, properties);

        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
            results.Add(entity);
        }

        return results;
    }

    #endregion

    #region IDisposable Implementation

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                try
                {
                    // 输出缓存统计
                    _logger.Info($"DuckDB元数据缓存统计: {DuckDBMetadataCache.GetStatistics()}");

                    _connection?.Close();
                    _connection?.Dispose();
                    _logger.Debug("DuckDB连接资源已释放");
                }
                catch (Exception ex)
                {
                    _logger.Error($"释放DuckDB连接资源时出错: {ex.Message}", ex);
                }
            }

            _disposed = true;
        }
    }

    ~DuckDBFileQueryProvider()
    {
        Dispose(false);
    }

    #endregion
}
