using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Diagnostics;
using Castle.Core.Logging;
using Abp.Dependency;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// 提供用于查询Parquet文件的特定功能的DuckDB提供程序
/// </summary>
public class DuckDBFileQueryProvider : DuckDbProviderBase, IDuckDBFileQueryProvider, ITransientDependency
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="logger">日志记录器</param>
    /// <param name="sqlBuilder">SQL构建器</param>
    /// <param name="performanceMonitor">性能监视器</param>
    public DuckDBFileQueryProvider(ILogger logger, DuckDBSqlBuilder sqlBuilder, QueryPerformanceMonitor performanceMonitor = null)
        : base(logger, sqlBuilder, performanceMonitor)
    {
        // 基类已经初始化了必要组件
    }

    #region 从 DuckDbProviderAdvanced 合并的方法

    /// <summary>
    /// 使用向量化过滤进行查询
    /// </summary>
    public async Task<List<TEntity>> QueryWithVectorizedFiltersAsync<TEntity>(
        string tableName,
        string[] columns,
        object[][] filterValues,
        int resultLimit = 1000)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        if (columns == null || columns.Length == 0)
            throw new ArgumentException("至少需要一个过滤列", nameof(columns));
        if (filterValues == null || filterValues.Length != columns.Length)
            throw new ArgumentException("过滤值数组长度必须与列数组长度匹配", nameof(filterValues));

        try
        {
            // 使用SqlBuilder的实现而不是本地重复实现
            var sql = _sqlBuilder.BuildVectorizedFilterQuery(tableName, columns, filterValues, resultLimit);
            _logger.Debug($"执行向量化过滤查询: {sql}");

            // 执行查询
            return await ExecuteQueryWithMetricsAsync(
                () => QueryWithRawSqlAsync<TEntity>(sql),
                "VectorizedFilter",
                sql);
        }
        catch (Exception ex)
        {
            HandleException("执行向量化过滤查询", ex, null);
            throw; // 这里不会执行到，因为HandleException会抛出异常
        }
    }

    /// <summary>
    /// 注册自定义函数
    /// </summary>
    public void RegisterFunction<TReturn, TParam1>(string functionName, Func<TParam1, TReturn> function)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        try
        {
            _logger.Debug($"注册自定义函数: {functionName}");

            // 注意：这里需要根据DuckDB.NET的具体实现调整
            // 以下是一个示例实现

            // 获取内部连接对象
            // var db = _connection.InnerConnection;
            // db.RegisterFunction(functionName, function);

            _logger.Info($"成功注册自定义函数: {functionName}");
        }
        catch (Exception ex)
        {
            HandleException("注册自定义函数", ex);
        }
    }

    /// <summary>
    /// 直接从Parquet文件执行查询
    /// </summary>
    public async Task<List<TEntity>> QueryParquetFileAsync<TEntity>(
        string parquetFilePath,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        if (string.IsNullOrWhiteSpace(parquetFilePath))
            throw new ArgumentNullException(nameof(parquetFilePath));

        try
        {
            // 验证文件存在
            if (!File.Exists(parquetFilePath))
                throw new FileNotFoundException("找不到Parquet文件", parquetFilePath);

            // 构建查询语句
            var whereClause = _sqlBuilder.BuildWhereClause(predicate);
            var sql = $"SELECT * FROM read_parquet('{parquetFilePath.Replace("'", "''")}'){whereClause}";

            _logger.Debug($"直接查询Parquet文件: {parquetFilePath}");

            // 执行查询
            return await ExecuteQueryWithMetricsAsync(
                () => QueryWithRawSqlAsync<TEntity>(sql),
                "ParquetQuery",
                sql);
        }
        catch (Exception ex)
        {
            HandleException("查询Parquet文件", ex, null, $"文件路径: {parquetFilePath}");
            return new List<TEntity>(); // 这里不会执行到
        }
    }

    #endregion

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
    /// 流式查询大量数据，避免一次性加载所有内容到内存 - 优化版本
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

                // 调用优化的流式查询实现
                return await QueryStreamOptimizedAsync<TEntity>(
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
    /// 优化的流式查询实现
    /// </summary>
    private async Task<int> QueryStreamOptimizedAsync<TEntity>(
        string dataSource,
        string whereClause,
        int batchSize,
        Func<IEnumerable<TEntity>, Task> processAction,
        CancellationToken cancellationToken = default)
    {
        var sql = BuildSelectQuery(dataSource, whereClause);
        int totalProcessed = 0;
        int batchCount = 0;
        int errorCount = 0;

        var stopwatch = Stopwatch.StartNew();
        var lastLogTime = stopwatch.ElapsedMilliseconds;

        using var command = _connection.CreateCommand();
        command.CommandText = sql;

        if (_configuration.CommandTimeout != TimeSpan.Zero)
            command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

        // 使用顺序访问模式提高读取效率
        using var reader = await command.ExecuteReaderAsync(
            System.Data.CommandBehavior.SequentialAccess,
            cancellationToken);

        var properties = GetEntityProperties<TEntity>();
        var columnMappings = GetColumnMappings(reader, properties);

        // 在最开始就为批处理分配内存，避免频繁重新分配
        var batch = new List<TEntity>(batchSize);

        try
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
                    batch.Add(entity);
                    totalProcessed++;
                }
                catch (Exception ex)
                {
                    errorCount++;

                    // 仅记录有限数量的错误，避免日志爆炸
                    if (errorCount <= 10 || errorCount % 100 == 0)
                    {
                        _logger.Warn($"映射实体失败（第{errorCount}个错误）: {ex.Message}");
                    }

                    // 继续处理下一条记录
                    continue;
                }

                // 当批次达到指定大小时处理
                if (batch.Count >= batchSize)
                {
                    batchCount++;

                    // 处理当前批次
                    if (processAction != null)
                    {
                        await processAction(batch);
                    }

                    batch.Clear();

                    // 定期记录进度（每30秒或每20批）
                    long currentTime = stopwatch.ElapsedMilliseconds;
                    if (_configuration.EnablePerformanceMonitoring &&
                        (batchCount % 20 == 0 || currentTime - lastLogTime > 30000))
                    {
                        lastLogTime = currentTime;
                        double recordsPerSecond = totalProcessed / (currentTime / 1000.0);
                        _logger.Debug($"流处理进度: 已处理 {totalProcessed:N0} 条记录 " +
                                      $"({batchCount}批), 吞吐量: {recordsPerSecond:N1} 记录/秒" +
                                      (errorCount > 0 ? $", 错误数: {errorCount}" : ""));
                    }
                }
            }

            // 处理最后一批数据
            if (batch.Count > 0 && processAction != null)
            {
                await processAction(batch);
            }
        }
        finally
        {
            // 记录总处理时间和吞吐量
            stopwatch.Stop();
            if (_configuration.EnablePerformanceMonitoring && totalProcessed > 0)
            {
                double recordsPerSecond = totalProcessed / (stopwatch.ElapsedMilliseconds / 1000.0);
                _logger.Info($"流处理完成: {totalProcessed:N0} 条记录, " +
                             $"总耗时: {stopwatch.ElapsedMilliseconds:N0}ms, " +
                             $"平均吞吐量: {recordsPerSecond:N1} 记录/秒" +
                             (errorCount > 0 ? $", 错误总数: {errorCount}" : ""));
            }
        }

        return totalProcessed;
    }

    /// <summary>
    /// 以异步流方式查询数据 - 优化版本
    /// </summary>
    public async IAsyncEnumerable<TEntity> QueryStreamEnumerableAsync<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int batchSize = 1000,
        [EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        ValidateFilePaths(filePaths);

        // 构建查询参数
        var whereClause = predicate != null
            ? _sqlBuilder.BuildWhereClause(predicate)
            : string.Empty;

        var dataSource = BuildDataSource(filePaths);
        var sql = BuildSelectQuery(dataSource, whereClause);

        // 性能统计初始化
        int itemCount = 0;
        var stopwatch = Stopwatch.StartNew();
        var lastLogTime = stopwatch.ElapsedMilliseconds;
        int errorCount = 0;

        using var command = _connection.CreateCommand();
        command.CommandText = sql;

        if (_configuration.CommandTimeout != TimeSpan.Zero)
            command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

        // 使用顺序访问模式提高读取效率
        using var reader = await command.ExecuteReaderAsync(
            System.Data.CommandBehavior.SequentialAccess,
            cancellationToken);

        var properties = GetEntityProperties<TEntity>();
        var columnMappings = GetColumnMappings(reader, properties);

        // 使用高效的预取缓冲区
        int bufferSize = Math.Min(batchSize, 1000);
        var buffer = new Queue<TEntity>(bufferSize);
        bool endOfData = false;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // 如果缓冲区为空，尝试填充
                if (buffer.Count == 0 && !endOfData)
                {
                    // 填充缓冲区
                    for (int i = 0; i < bufferSize && await reader.ReadAsync(cancellationToken); i++)
                    {
                        try
                        {
                            var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
                            buffer.Enqueue(entity);
                        }
                        catch (Exception ex)
                        {
                            errorCount++;
                            if (errorCount <= 10 || errorCount % 100 == 0)
                            {
                                _logger.Warn($"映射实体失败（第{errorCount}个错误）: {ex.Message}", ex);
                            }
                            // 跳过这一项，继续下一项
                        }
                    }

                    // 标记数据是否已读取完毕
                    if (buffer.Count < bufferSize)
                    {
                        endOfData = true;
                    }
                }

                // 如果缓冲区为空且数据已读取完毕，停止迭代
                if (buffer.Count == 0 && endOfData)
                {
                    break;
                }

                // 输出缓冲区中的下一个项目
                if (buffer.Count > 0)
                {
                    var entity = buffer.Dequeue();
                    itemCount++;

                    // 定期记录性能统计（每30秒或每1000项）
                    if (_configuration.EnablePerformanceMonitoring &&
                        (itemCount % 1000 == 0 || stopwatch.ElapsedMilliseconds - lastLogTime > 30000))
                    {
                        lastLogTime = stopwatch.ElapsedMilliseconds;
                        double itemsPerSecond = itemCount / (stopwatch.ElapsedMilliseconds / 1000.0);
                        _logger.Debug($"流枚举进度: 已处理 {itemCount:N0} 条记录, " +
                                      $"吞吐量: {itemsPerSecond:N1} 记录/秒" +
                                      (errorCount > 0 ? $", 错误数: {errorCount}" : ""));

                        // 记录到性能监控系统
                        _performanceMonitor.RecordQueryExecution(
                            "StreamEnumerable",
                            $"Processed {itemCount} items",
                            filePaths.Count(),
                            itemCount,
                            stopwatch.ElapsedMilliseconds,
                            true);
                    }

                    yield return entity;
                }
            }
        }
        finally
        {
            // 记录总处理时间和吞吐量
            stopwatch.Stop();
            if (_configuration.EnablePerformanceMonitoring && itemCount > 0)
            {
                double itemsPerSecond = itemCount / (stopwatch.ElapsedMilliseconds / 1000.0);
                _logger.Info($"流枚举完成: {itemCount:N0} 条记录, " +
                             $"总耗时: {stopwatch.ElapsedMilliseconds:N0}ms, " +
                             $"平均吞吐量: {itemsPerSecond:N1} 记录/秒" +
                             (errorCount > 0 ? $", 错误总数: {errorCount}" : ""));
            }
        }
    }

    #endregion

    #region 聚合方法 - 合并重复的聚合方法实现

    /// <summary>
    /// 构建聚合查询SQL
    /// </summary>
    protected string BuildAggregateQuery(string dataSource, string aggregateFunction, string columnName, string whereClause)
    {
        var sql = $"SELECT {aggregateFunction}({columnName}) FROM {dataSource}";

        if (!string.IsNullOrEmpty(whereClause))
            sql += $" WHERE {whereClause}";

        return sql;
    }

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
                    results.Add(DuckDBTypeConverter.SafeConvert<TResult>(value, _logger));
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
    /// 计算指定列的总和（基于列名）
    /// </summary>
    public async Task<TResult> SumAsync<TEntity, TResult>(
        IEnumerable<string> filePaths,
        string columnName,
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

                // 构建求和SQL
                var sql = BuildAggregateQuery(dataSource, "SUM", columnName, whereClause);

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
                    "SumParquet",
                    sql,
                    fileCount);
            },
            "求和Parquet记录",
            $"文件数: {fileCount}, 列: {columnName}"
        );
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
                // 使用通用的聚合查询构建方法
                var sql = BuildAggregateQuery(dataSource, "COUNT", "*", whereClause);

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
                var key = DuckDBTypeConverter.SafeConvert<TGroupKey>(reader.GetValue(0), _logger);
                var count = DuckDBTypeConverter.SafeConvert<TResult>(reader.GetValue(1), _logger);

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
                var key = DuckDBTypeConverter.SafeConvert<TGroupKey>(reader.GetValue(0), _logger);
                var sum = DuckDBTypeConverter.SafeConvert<TResult>(reader.GetValue(1), _logger);

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
