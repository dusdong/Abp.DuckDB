using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using Abp.Dependency;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB文件查询提供程序实现
/// </summary>
public class DuckDBFileQueryProvider : DuckDbProviderAdvanced, IDuckDBFileQueryProvider, ITransientDependency
{
    #region 构造函数

    public DuckDBFileQueryProvider(ILogger logger)
        : base(logger)
    {
    }

    #endregion

    #region 文件查询特定方法

    /// <summary>
    /// 查询特定类型的存档数据
    /// </summary>
    public async Task<List<TEntity>> QueryAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        try
        {
            // 获取实体类型的属性信息，用于列投影（使用缓存）
            var selectedColumns = GetEntityColumns<TEntity>();

            // 使用优化后的SQL表达式访问器
            string sql = BuildDirectParquetQuery<TEntity>(filePaths, predicate, selectedColumns);

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

        // 使用配置的批处理大小限制
        batchSize = Math.Min(batchSize, _configuration.MaxBatchSize);

        try
        {
            // 获取实体类型的属性信息，用于列投影（使用缓存）
            var selectedColumns = GetEntityColumns<TEntity>();

            // 使用优化后的SQL表达式访问器
            string sql = BuildDirectParquetQuery<TEntity>(filePaths, predicate, selectedColumns);

            var stopwatch = Stopwatch.StartNew();
            int totalProcessed = 0;

            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            // 开启流式模式
            command.UseStreamingMode = _configuration.UseStreamingModeByDefault;

            // 设置命令超时
            if (_configuration.CommandTimeout != TimeSpan.Zero)
            {
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
            }

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

            // 记录性能指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                _performanceMonitor.RecordQueryExecution(
                    "QueryStream",
                    sql,
                    filePaths.Count(),
                    totalProcessed,
                    stopwatch.ElapsedMilliseconds,
                    true);

                // 检查是否是慢查询
                if (_configuration.LogSlowQueries && stopwatch.ElapsedMilliseconds > _configuration.SlowQueryThresholdMs)
                {
                    _logger.Warn($"[慢查询] 流式查询耗时: {stopwatch.ElapsedMilliseconds}ms, SQL: {sql}");
                }
            }

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

        // 使用配置的批处理大小限制
        batchSize = Math.Min(batchSize, _configuration.MaxBatchSize);

        // 获取实体类型的属性信息，用于列投影（使用缓存）
        var selectedColumns = GetEntityColumns<TEntity>();

        // 使用优化后的SQL表达式访问器
        string sql = BuildDirectParquetQuery<TEntity>(filePaths, predicate, selectedColumns);

        var stopwatch = Stopwatch.StartNew();
        int processedCount = 0;

        try
        {
            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            // 开启流式模式
            command.UseStreamingMode = _configuration.UseStreamingModeByDefault;

            // 设置命令超时
            if (_configuration.CommandTimeout != TimeSpan.Zero)
            {
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
            }

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
                if (processedCount % batchSize == 0 && _configuration.CollectDetailedMetrics)
                {
                    _logger.Debug($"[DuckDB流式进度] 已处理记录: {processedCount}");
                }
            }
        }
        finally
        {
            stopwatch.Stop();

            // 记录性能指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                _performanceMonitor.RecordQueryExecution(
                    "QueryStreamEnumerable",
                    sql,
                    filePaths.Count(),
                    processedCount,
                    stopwatch.ElapsedMilliseconds,
                    true);

                // 检查是否是慢查询
                if (_configuration.LogSlowQueries && stopwatch.ElapsedMilliseconds > _configuration.SlowQueryThresholdMs)
                {
                    _logger.Warn($"[慢查询] 流式枚举查询耗时: {stopwatch.ElapsedMilliseconds}ms, SQL: {sql}");
                }
            }

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
            // 获取实体类型的属性信息，用于列投影
            var selectedColumns = GetEntityColumns<TEntity>();

            // 构建所有查询的SQL
            var queryScripts = new List<string>();
            foreach (var entry in predicatesMap)
            {
                string sql = BuildDirectParquetQuery<TEntity>(filePaths, entry.Value, selectedColumns);
                queryScripts.Add(sql);
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

            // 记录性能指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                int totalResults = results.Values.Sum(v => v.Count);
                _performanceMonitor.RecordQueryExecution(
                    "BatchQuery",
                    string.Join("; ", queryScripts),
                    filePaths.Count(),
                    totalResults,
                    stopwatch.ElapsedMilliseconds,
                    true);
            }

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

        var stopwatch = Stopwatch.StartNew();

        // 使用事务来优化批处理
        using var transaction = _connection.BeginTransaction();
        try
        {
            foreach (var sql in sqlQueries)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = sql;
                command.Transaction = transaction;

                // 设置命令超时
                if (_configuration.CommandTimeout != TimeSpan.Zero)
                {
                    command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                }

                var queryResult = await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
                results.Add(queryResult);
            }

            transaction.Commit();

            stopwatch.Stop();

            // 记录详细指标
            if (_configuration.CollectDetailedMetrics)
            {
                _logger.Debug($"[DuckDB批处理详情] 执行了 {sqlQueries.Count} 个查询, " +
                              $"共返回 {results.Sum(r => r.Count)} 条记录, " +
                              $"耗时: {stopwatch.ElapsedMilliseconds}ms");
            }

            return results;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
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

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 构建单个SQL查询，同时计算多个指标
            var aggregations = string.Join(", ",
                metricsMap.Select(m => $"{m.Value} AS {m.Key}"));

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建数据源子句
            string fromClause = BuildParquetSourceClause(filePaths);

            // 构建完整SQL
            string sql = $"SELECT {aggregations} FROM {fromClause}";
            if (!string.IsNullOrEmpty(whereClause))
                sql += $" WHERE {whereClause}";

            // 执行查询
            var result = new Dictionary<string, decimal>();

            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            // 设置命令超时
            if (_configuration.CommandTimeout != TimeSpan.Zero)
            {
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
            }

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

            // 记录性能指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                _performanceMonitor.RecordQueryExecution(
                    "MultipleMetrics",
                    sql,
                    filePaths.Count(),
                    result.Count,
                    stopwatch.ElapsedMilliseconds,
                    true);
            }

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
            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

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

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        countCommand.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync());
                }

                if (totalCount > 0)
                {
                    // 执行分页查询
                    using (var pageCommand = _connection.CreateCommand())
                    {
                        pageCommand.CommandText = pagingSql;
                        pageCommand.Transaction = transaction;

                        // 设置命令超时
                        if (_configuration.CommandTimeout != TimeSpan.Zero)
                        {
                            pageCommand.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                        }

                        items = await ExecuteReaderAsync<TEntity>(pageCommand);
                    }
                }

                transaction.Commit();
            }

            stopwatch.Stop();

            // 记录性能指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                _performanceMonitor.RecordQueryExecution(
                    "QueryPaged",
                    $"Count: {countSql}; Page: {pagingSql}",
                    filePaths.Count(),
                    items.Count,
                    stopwatch.ElapsedMilliseconds,
                    true);

                // 检查是否是慢查询
                if (_configuration.LogSlowQueries && stopwatch.ElapsedMilliseconds > _configuration.SlowQueryThresholdMs)
                {
                    _logger.Warn($"[慢查询] 分页查询耗时: {stopwatch.ElapsedMilliseconds}ms, 页码: {pageIndex}, 页大小: {pageSize}");
                }
            }

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
            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建计数查询
            string countSql = BuildDirectParquetCountQuery(filePaths, whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = countSql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

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
    /// 求和
    /// </summary>
    public async Task<decimal> SumAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求和的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"SUM({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

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
    /// 求平均值
    /// </summary>
    public async Task<decimal> AvgAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求平均值的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"AVG({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

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
    /// 求最小值
    /// </summary>
    public async Task<decimal> MinAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求最小值的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"MIN({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

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
    /// 求最大值
    /// </summary>
    public async Task<decimal> MaxAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求最大值的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"MAX({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

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

    #endregion

    #region 分组聚合

    /// <summary>
    /// 执行分组聚合查询
    /// </summary>
    /// <typeparam name="TResult">结果类型</typeparam>
    /// <param name="filePaths">要查询的文件列表</param>
    /// <param name="selectClause">SELECT子句（不包含SELECT关键字）</param>
    /// <param name="whereClause">WHERE子句（不包含WHERE关键字），可选</param>
    /// <param name="groupByClause">GROUP BY子句（不包含GROUP BY关键字），可选</param>
    /// <param name="orderByClause">ORDER BY子句（不包含ORDER BY关键字），可选</param>
    /// <param name="parameters">查询参数，可选</param>
    /// <param name="cancellationToken">取消令牌，可选</param>
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

        if (string.IsNullOrEmpty(selectClause))
            throw new ArgumentException("必须提供SELECT子句", nameof(selectClause));

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 构建查询
            var queryBuilder = new StringBuilder();
            queryBuilder.Append($"SELECT {selectClause} FROM {BuildParquetSourceClause(filePaths)}");

            if (!string.IsNullOrEmpty(whereClause))
                queryBuilder.Append($" WHERE {whereClause}");

            if (!string.IsNullOrEmpty(groupByClause))
                queryBuilder.Append($" GROUP BY {groupByClause}");

            if (!string.IsNullOrEmpty(orderByClause))
                queryBuilder.Append($" ORDER BY {orderByClause}");

            string sql = queryBuilder.ToString();

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    // 添加参数
                    if (parameters != null)
                    {
                        foreach (var param in parameters)
                        {
                            var parameter = command.CreateParameter();
                            parameter.ParameterName = param.Key;
                            parameter.Value = param.Value;
                            command.Parameters.Add(parameter);
                        }
                    }

                    return await ExecuteReaderAsync<TResult>(command, cancellationToken).ConfigureAwait(false);
                },
                "GroupByQuery",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"执行分组聚合查询失败: {ex.Message}", ex);
            throw;
        }
    }

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
    public async Task<List<(TGroupKey Key, TResult Count)>> CountByAsync<TEntity, TGroupKey, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TGroupKey>> groupExpression,
        Expression<Func<TEntity, bool>> predicate = null,
        string orderByColumn = null,
        bool ascending = true,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        if (groupExpression == null)
            throw new ArgumentNullException(nameof(groupExpression));

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 获取分组字段名称
            string groupColumnName = GetColumnName(groupExpression);

            // 构建WHERE子句
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建完整SQL
            var queryBuilder = new StringBuilder();
            queryBuilder.Append($"SELECT {groupColumnName} as Key, COUNT(*) as Count ");
            queryBuilder.Append($"FROM {BuildParquetSourceClause(filePaths)} ");

            if (!string.IsNullOrEmpty(whereClause))
                queryBuilder.Append($"WHERE {whereClause} ");

            queryBuilder.Append($"GROUP BY {groupColumnName} ");

            if (!string.IsNullOrEmpty(orderByColumn))
                queryBuilder.Append($"ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}");

            string sql = queryBuilder.ToString();

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    return await ExecuteReaderAsync<(TGroupKey, TResult)>(command, cancellationToken).ConfigureAwait(false);
                },
                "CountBy",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"执行分组计数查询失败: {ex.Message}", ex);
            throw;
        }
    }

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

        if (sumExpression == null)
            throw new ArgumentNullException(nameof(sumExpression));

        if (groupExpression == null)
            throw new ArgumentNullException(nameof(groupExpression));

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 获取求和和分组字段名称
            string sumColumnName = GetColumnName(sumExpression);
            string groupColumnName = GetColumnName(groupExpression);

            // 构建WHERE子句
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建完整SQL
            var queryBuilder = new StringBuilder();
            queryBuilder.Append($"SELECT {groupColumnName} as Key, SUM({sumColumnName}) as Sum ");
            queryBuilder.Append($"FROM {BuildParquetSourceClause(filePaths)} ");

            if (!string.IsNullOrEmpty(whereClause))
                queryBuilder.Append($"WHERE {whereClause} ");

            queryBuilder.Append($"GROUP BY {groupColumnName} ");

            if (!string.IsNullOrEmpty(orderByColumn))
                queryBuilder.Append($"ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}");

            string sql = queryBuilder.ToString();

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    return await ExecuteReaderAsync<(TGroupKey, TResult)>(command, cancellationToken).ConfigureAwait(false);
                },
                "SumBy",
                sql,
                filePaths.Count());
        }
        catch (Exception ex)
        {
            _logger.Error($"执行分组求和查询失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 求和并返回指定类型结果
    /// </summary>
    public async Task<TResult> SumAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求和的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"SUM({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    var result = await command.ExecuteScalarAsync(cancellationToken);

                    // 使用安全转换替代直接转换
                    return SafeConvert<TResult>(result);
                },
                "SumTyped",
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
    /// 计数并返回指定类型结果
    /// </summary>
    public async Task<TResult> CountAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        try
        {
            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建计数查询
            string countSql = BuildDirectParquetCountQuery(filePaths, whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = countSql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    var result = await command.ExecuteScalarAsync(cancellationToken);

                    // 使用安全转换替代直接转换
                    return SafeConvert<TResult>(result);
                },
                "CountTyped",
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
    /// 求最小值并返回指定类型结果
    /// </summary>
    public async Task<TResult> MinAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求最小值的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"MIN({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    var result = await command.ExecuteScalarAsync(cancellationToken);

                    // 使用安全转换替代直接转换
                    return SafeConvert<TResult>(result);
                },
                "MinTyped",
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
    /// 求最大值并返回指定类型结果
    /// </summary>
    public async Task<TResult> MaxAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, TResult>> selector,
        Expression<Func<TEntity, bool>> predicate = null,
        CancellationToken cancellationToken = default)
    {
        ValidateFilePaths(filePaths);

        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        try
        {
            // 获取待求最大值的属性名
            string columnName = GetColumnName(selector);

            // 使用优化后的SQL表达式访问器
            string whereClause = string.Empty;
            if (predicate != null)
            {
                var visitor = new SqlExpressionVisitor(_logger);
                whereClause = visitor.Translate(predicate);
            }

            // 构建聚合查询
            string sql = BuildDirectParquetAggregateQuery(filePaths, $"MAX({columnName})", whereClause);

            return await ExecuteQueryWithMetricsAsync(
                async () =>
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

                    var result = await command.ExecuteScalarAsync(cancellationToken);

                    // 使用安全转换替代直接转换
                    return SafeConvert<TResult>(result);
                },
                "MaxTyped",
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
    /// 安全地将对象转换为指定类型
    /// </summary>
    private TResult SafeConvert<TResult>(object value)
    {
        if (value == null || value == DBNull.Value)
            return default!;

        Type targetType = typeof(TResult);

        try
        {
            // 处理常见的数值类型
            if (targetType == typeof(int) || targetType == typeof(int?))
                return (TResult)(object)Convert.ToInt32(value);

            if (targetType == typeof(long) || targetType == typeof(long?))
                return (TResult)(object)Convert.ToInt64(value);

            if (targetType == typeof(decimal) || targetType == typeof(decimal?))
                return (TResult)(object)Convert.ToDecimal(value);

            if (targetType == typeof(double) || targetType == typeof(double?))
                return (TResult)(object)Convert.ToDouble(value);

            if (targetType == typeof(float) || targetType == typeof(float?))
                return (TResult)(object)Convert.ToSingle(value);

            // 如果是字符串，直接转换
            if (targetType == typeof(string))
                return (TResult)(object)value.ToString();

            // 如果目标类型实现了IConvertible接口，使用正常的转换
            if (typeof(IConvertible).IsAssignableFrom(targetType))
                return (TResult)Convert.ChangeType(value, targetType);

            // 原始类型和类型兼容的情况
            if (value.GetType() == targetType || targetType.IsAssignableFrom(value.GetType()))
                return (TResult)value;

            // 尝试使用反射进行转换
            var converter = TypeDescriptor.GetConverter(value.GetType());
            if (converter != null && converter.CanConvertTo(targetType))
                return (TResult)converter.ConvertTo(value, targetType);

            converter = TypeDescriptor.GetConverter(targetType);
            if (converter != null && converter.CanConvertFrom(value.GetType()))
                return (TResult)converter.ConvertFrom(value);

            // 记录警告并返回默认值
            _logger.Warn($"无法将类型 {value.GetType().Name} 转换为 {targetType.Name}，返回默认值");
            return default!;
        }
        catch (Exception ex)
        {
            _logger.Error($"类型转换失败: {ex.Message}, 源类型: {value.GetType().Name}, 目标类型: {targetType.Name}");
            return default!;
        }
    }

    /// <summary>
    /// 执行查询并返回指定类型的结果列表
    /// </summary>
    private async Task<List<TResult>> ExecuteReaderAsync<TResult>(
        DuckDBCommand command,
        CancellationToken cancellationToken = default)
    {
        var results = new List<TResult>();

        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var resultType = typeof(TResult);
        var isTuple = resultType.Name.Contains("Tuple") || resultType.Name.Contains("ValueTuple");

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (isTuple)
            {
                // 处理元组类型的返回值
                object result = CreateTupleFromReader(reader, resultType);
                results.Add((TResult)result);
            }
            else
            {
                // 处理普通类型的返回值
                results.Add(MapReaderToValue<TResult>(reader));
            }
        }

        return results;
    }

    /// <summary>
    /// 从数据读取器创建元组
    /// </summary>
    private object CreateTupleFromReader(DbDataReader reader, Type tupleType)
    {
        var tupleFields = tupleType.GetFields();
        var fieldValues = new object[tupleFields.Length];

        for (int i = 0; i < tupleFields.Length && i < reader.FieldCount; i++)
        {
            var fieldType = tupleFields[i].FieldType;
            var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

            if (value != null && value != DBNull.Value)
            {
                fieldValues[i] = Convert.ChangeType(value, fieldType);
            }
        }

        return Activator.CreateInstance(tupleType, fieldValues);
    }

    /// <summary>
    /// 将数据读取器的第一列映射为指定类型的值
    /// </summary>
    private TResult MapReaderToValue<TResult>(DbDataReader reader)
    {
        if (reader.IsDBNull(0))
            return default;

        var value = reader.GetValue(0);
        return SafeConvert<TResult>(value); // 使用安全转换替代直接转换
    }

    #endregion

    #region 文件SQL构建

    /// <summary>
    /// 验证文件路径
    /// </summary>
    private void ValidateFilePaths(IEnumerable<string> filePaths)
    {
        if (filePaths == null)
            throw new ArgumentNullException(nameof(filePaths));

        if (!filePaths.Any())
            throw new ArgumentException("必须提供至少一个文件路径", nameof(filePaths));
    }

    /// <summary>
    /// 直接构建查询SQL，不使用参数化
    /// </summary>
    private string BuildDirectParquetQuery<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        IEnumerable<string> selectedColumns = null)
    {
        string whereClause = string.Empty;
        if (predicate != null)
        {
            var visitor = new SqlExpressionVisitor(_logger);
            whereClause = visitor.Translate(predicate);
        }

        return BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);
    }

    /// <summary>
    /// 构建直接的Parquet查询，使用列表语法
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

        // 使用列表语法
        string sql = $"SELECT {columns} FROM {BuildParquetSourceClause(filePaths)}";

        // 添加WHERE子句（如果有）
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        return sql;
    }

    /// <summary>
    /// 构建直接的Parquet计数查询
    /// </summary>
    private string BuildDirectParquetCountQuery(IEnumerable<string> filePaths, string whereClause)
    {
        string sql = $"SELECT COUNT(*) FROM {BuildParquetSourceClause(filePaths)}";

        // 添加WHERE子句（如果有）
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        return sql;
    }

    /// <summary>
    /// 构建直接的Parquet聚合查询
    /// </summary>
    private string BuildDirectParquetAggregateQuery(
        IEnumerable<string> filePaths,
        string aggregateFunction,
        string whereClause)
    {
        string sql = $"SELECT {aggregateFunction} FROM {BuildParquetSourceClause(filePaths)}";

        // 添加WHERE子句（如果有）
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        return sql;
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
        // 构建基本查询
        string sql = BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);

        // 添加排序
        if (!string.IsNullOrEmpty(orderByColumn))
        {
            sql += $" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}";
        }

        // 添加分页
        sql += $" LIMIT {limit} OFFSET {offset}";

        return sql;
    }

    /// <summary>
    /// 构建Parquet数据源子句
    /// </summary>
    private string BuildParquetSourceClause(IEnumerable<string> filePaths)
    {
        // 使用文件名生成SQL UNION ALL查询
        if (filePaths.Count() == 1)
        {
            // 单文件情况
            return $"read_parquet('{EscapePath(filePaths.First())}')";
        }
        else
        {
            // 多文件情况，使用列表语法
            var fileList = string.Join(", ", filePaths.Select(p => $"'{EscapePath(p)}'"));
            return $"read_parquet([{fileList}])";
        }
    }

    /// <summary>
    /// 转义路径中的特殊字符
    /// </summary>
    private string EscapePath(string path)
    {
        // 避免SQL注入和特殊字符问题
        return path.Replace("'", "''");
    }

    #endregion
}
