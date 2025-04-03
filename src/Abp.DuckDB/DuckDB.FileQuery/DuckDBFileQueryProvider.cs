using System.Collections.Concurrent;
using System.Data;
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
    #region 私有字段

    // 连接池相关
    private static DuckDBConnectionPool _connectionPool;
    private static readonly object _poolInitLock = new object();
    private static Timer _maintenanceTimer;

    // 连接相关
    private DuckDBConnection _connection;
    private readonly ILogger _logger;
    private bool _disposed = false;
    private DuckDBConfiguration _configuration;
    private PooledConnection _pooledConnection;

    // 添加预编译语句缓存
    private readonly ConcurrentDictionary<string, PreparedStatementWrapper> _statementCache = new();

    #endregion

    #region 内部类

    // 预编译语句包装器，用于管理预编译语句的生命周期
    private class PreparedStatementWrapper : IDisposable
    {
        public DuckDBCommand Command { get; }
        private bool _disposed = false;

        public PreparedStatementWrapper(DuckDBCommand command)
        {
            Command = command;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Command?.Dispose();
                _disposed = true;
            }
        }
    }

    #endregion

    public DuckDBFileQueryProvider(ILogger logger)
    {
        _logger = logger ?? NullLogger.Instance;
        _configuration = DuckDBConfiguration.HighPerformance(); // 使用默认配置
    }

    #region 初始化方法

    /// <summary>
    /// 初始化DuckDB查询提供程序
    /// </summary>
    public void Initialize(string connectionString)
    {
        var config = new DuckDBConfiguration { ConnectionString = connectionString };
        Initialize(config);
    }

    /// <summary>
    /// 使用配置初始化DuckDB查询提供程序
    /// </summary>
    public void Initialize(DuckDBConfiguration configuration)
    {
        try
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            _configuration = configuration;

            // 应用配置到缓存系统
            DuckDBMetadataCache.ApplyConfiguration(configuration);

            if (_configuration.UseConnectionPool)
            {
                // 初始化连接池并获取连接
                InitializeConnectionPool();
                _pooledConnection = _connectionPool.GetConnectionAsync().GetAwaiter().GetResult();
                _connection = _pooledConnection.Connection;

                _logger.Info($"DuckDB查询提供程序初始化成功，使用连接池，线程数：{configuration.ThreadCount}，内存限制：{configuration.MemoryLimit}");

                // 初始化维护计时器
                if (_maintenanceTimer == null)
                {
                    _maintenanceTimer = new Timer(
                        _ => PerformPoolMaintenance(),
                        null,
                        TimeSpan.FromMinutes(5),
                        TimeSpan.FromMinutes(5));
                }
            }
            else
            {
                // 使用配置的连接字符串
                _connection = new DuckDBConnection(configuration.ConnectionString);
                _connection.Open();

                using (var cmd = _connection.CreateCommand())
                {
                    // 设置线程数
                    cmd.CommandText = $"PRAGMA threads={configuration.ThreadCount};";
                    cmd.ExecuteNonQuery();

                    // 内存管理配置
                    if (!string.IsNullOrEmpty(configuration.MemoryLimit))
                    {
                        cmd.CommandText = $"PRAGMA memory_limit='{configuration.MemoryLimit}';";
                        cmd.ExecuteNonQuery();
                    }

                    // 压缩配置
                    if (configuration.EnableCompression)
                    {
                        cmd.CommandText = $"PRAGMA force_compression='{configuration.CompressionType}';";
                        cmd.ExecuteNonQuery();
                    }
                }

                _logger.Info($"DuckDB查询提供程序初始化成功，线程数：{configuration.ThreadCount}，内存限制：{configuration.MemoryLimit}");
            }
        }
        catch (Exception ex)
        {
            _logger.Error("初始化DuckDB查询提供程序失败", ex);
            throw;
        }
    }

    /// <summary>
    /// 初始化连接池
    /// </summary>
    private void InitializeConnectionPool()
    {
        if (_connectionPool == null)
        {
            lock (_poolInitLock)
            {
                if (_connectionPool == null)
                {
                    var poolOptions = new DuckDBPoolOptions
                    {
                        ConnectionString = _configuration.ConnectionString,
                        MinConnections = _configuration.MinConnections,
                        MaxConnections = _configuration.MaxConnections,
                        ThreadsPerConnection = _configuration.ThreadCount,
                        MemoryLimit = _configuration.MemoryLimit,
                        MaxIdleTimeSeconds = _configuration.MaxIdleTimeSeconds,
                        MaxConnectionLifetimeHours = _configuration.MaxConnectionLifetimeHours,
                        EnableCompression = _configuration.EnableCompression,
                        CompressionType = _configuration.CompressionType
                    };
                    _connectionPool = new DuckDBConnectionPool(poolOptions, _logger);
                }
            }
        }
    }

    /// <summary>
    /// 执行连接池维护
    /// </summary>
    private static void PerformPoolMaintenance()
    {
        _connectionPool?.CleanIdleConnections();
    }

    /// <summary>
    /// 获取连接池状态信息
    /// </summary>
    public PoolStatus GetConnectionPoolStatus()
    {
        return _connectionPool?.GetStatus();
    }

    #endregion

    #region 语句缓存

    /// <summary>
    /// 获取或创建预编译语句
    /// </summary>
    private PreparedStatementWrapper GetOrCreatePreparedStatement(string sql, bool forceCreate = false)
    {
        // 如果强制创建新实例，则不使用缓存
        if (forceCreate || !_configuration.EnableCache)
        {
            var command = _connection.CreateCommand();
            command.CommandText = sql;

            // 设置命令超时
            if (_configuration.CommandTimeout != TimeSpan.Zero)
            {
                command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
            }

            try
            {
                command.Prepare(); // 预编译语句
            }
            catch (Exception ex)
            {
                _logger.Warn($"预编译语句失败 (非致命): {ex.Message}");
                // 继续使用未预编译的命令
            }

            return new PreparedStatementWrapper(command);
        }

        // 从缓存获取或创建
        return _statementCache.GetOrAdd(sql, key =>
        {
            try
            {
                var command = _connection.CreateCommand();
                command.CommandText = key;

                // 设置命令超时
                if (_configuration.CommandTimeout != TimeSpan.Zero)
                {
                    command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                }

                try
                {
                    command.Prepare(); // 预编译语句
                }
                catch (Exception ex)
                {
                    _logger.Warn($"预编译语句失败 (非致命): {ex.Message}");
                    // 继续使用未预编译的命令
                }

                return new PreparedStatementWrapper(command);
            }
            catch (Exception ex)
            {
                _logger.Error($"创建预编译语句失败: {ex.Message}, SQL: {key}", ex);
                throw;
            }
        });
    }

    #endregion

    #region 查询方法

    /// <summary>
    /// 阶段一优化：添加查询分析功能
    /// </summary>
    public async Task<string> AnalyzeQueryPlanAsync(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = $"EXPLAIN {sql}";

        if (_configuration.LogQueryPlans)
        {
            var plan = (string)await command.ExecuteScalarAsync();
            _logger.Debug($"查询计划: {plan}");
            return plan;
        }

        return (string)await command.ExecuteScalarAsync();
    }

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

            // 记录查询失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "Query",
                    $"查询失败: {ex.Message}",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

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
                QueryPerformanceMonitor.RecordQueryExecution(
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

            // 记录查询失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "QueryStream",
                    $"流式查询失败: {ex.Message}",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

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
                QueryPerformanceMonitor.RecordQueryExecution(
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
                QueryPerformanceMonitor.RecordQueryExecution(
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "BatchQuery",
                    "批量查询失败",
                    filePaths.Count(),
                    0,
                    stopwatch.ElapsedMilliseconds,
                    false,
                    ex);
            }

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
                QueryPerformanceMonitor.RecordQueryExecution(
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "MultipleMetrics",
                    "多指标查询失败",
                    filePaths.Count(),
                    0,
                    stopwatch.ElapsedMilliseconds,
                    false,
                    ex);
            }

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
                QueryPerformanceMonitor.RecordQueryExecution(
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "QueryPaged",
                    "分页查询失败",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "Count",
                    "统计查询失败",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

            throw;
        }
    }

    /// <summary>
    /// 获取特定列的合计值
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "Sum",
                    "求和查询失败",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

            throw;
        }
    }

    /// <summary>
    /// 获取特定列的平均值
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "Avg",
                    "求平均值查询失败",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

            throw;
        }
    }

    /// <summary>
    /// 获取特定列的最小值
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "Min",
                    "求最小值查询失败",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

            throw;
        }
    }

    /// <summary>
    /// 获取特定列的最大值
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

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "Max",
                    "求最大值查询失败",
                    filePaths.Count(),
                    0,
                    0,
                    false,
                    ex);
            }

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

                    // 设置命令超时
                    if (_configuration.CommandTimeout != TimeSpan.Zero)
                    {
                        command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;
                    }

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
            _logger.Error($"执行自定义SQL查询失败: {ex.Message}，SQL语句：{sql}", ex);

            // 记录失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    "RawSql",
                    sql,
                    0,
                    0,
                    0,
                    false,
                    ex);
            }

            throw new Exception($"执行自定义SQL查询失败", ex);
        }
    }

    #endregion

    #region 监控和指标

    /// <summary>
    /// 获取性能报告
    /// </summary>
    public QueryPerformanceMonitor.PerformanceReport GetPerformanceReport()
    {
        return QueryPerformanceMonitor.GenerateReport();
    }

    /// <summary>
    /// 获取查询类型的性能指标
    /// </summary>
    public QueryPerformanceMonitor.QueryMetrics GetMetricsForQueryType(string queryType)
    {
        return QueryPerformanceMonitor.GetMetricsForQueryType(queryType);
    }

    /// <summary>
    /// 重置性能指标
    /// </summary>
    public void ResetPerformanceMetrics()
    {
        QueryPerformanceMonitor.ResetMetrics();
    }

    #endregion

    #region 缓存管理

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
    /// <param name="entityTypes">需要预热的实体类型集合</param>
    public void PrewarmEntityMetadata(params Type[] entityTypes)
    {
        if (entityTypes == null || entityTypes.Length == 0) return;

        _logger.Debug("开始预热DuckDB元数据缓存");
        foreach (var entityType in entityTypes)
        {
            DuckDBMetadataCache.PrewarmEntityMetadata(entityType);
        }

        _logger.Debug($"DuckDB元数据缓存预热完成，共预热 {entityTypes.Length} 个实体类型");
    }

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
    /// <param name="entityTypes">需要预热的实体类型集合</param>
    public void PrewarmEntityMetadata(IEnumerable<Type> entityTypes)
    {
        if (entityTypes == null) return;

        _logger.Debug("开始预热DuckDB元数据缓存");
        int count = 0;
        foreach (var entityType in entityTypes)
        {
            DuckDBMetadataCache.PrewarmEntityMetadata(entityType);
            count++;
        }

        _logger.Debug($"DuckDB元数据缓存预热完成，共预热 {count} 个实体类型");

        // 输出连接池状态（如果使用连接池）
        if (_configuration.UseConnectionPool && _connectionPool != null)
        {
            var status = _connectionPool.GetStatus();
            _logger.Debug($"DuckDB连接池状态: 总连接数: {status.TotalConnections}, " +
                          $"可用连接: {status.AvailableConnections}, " +
                          $"使用中连接: {status.BusyConnections}");
        }
    }

    /// <summary>
    /// 手动清理缓存
    /// </summary>
    /// <param name="evictionPercentage">要清除的缓存百分比 (0-100)</param>
    public void CleanupCache(int evictionPercentage = 20)
    {
        _logger.Debug($"手动清理缓存，清理比例: {evictionPercentage}%");
        DuckDBMetadataCache.ManualCleanup(evictionPercentage);
        _logger.Info($"缓存清理完成，当前缓存状态: {DuckDBMetadataCache.GetStatistics()}");
    }

    /// <summary>
    /// 清空所有缓存
    /// </summary>
    public void ClearAllCaches()
    {
        _logger.Debug("清空所有元数据缓存");
        DuckDBMetadataCache.ClearCache();
        _logger.Info("所有元数据缓存已清空");
    }

    /// <summary>
    /// 获取缓存统计信息
    /// </summary>
    /// <returns>缓存统计信息字符串</returns>
    public string GetCacheStatistics()
    {
        return DuckDBMetadataCache.GetStatistics();
    }

    #endregion

    #region SQL构建

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

    private string BuildParquetSourceClause(IEnumerable<string> filePaths)
    {
        var escapedPaths = filePaths.Select(p => $"'{p.Replace("'", "''")}'");
        string pathList = string.Join(", ", escapedPaths);
        return $"read_parquet([{pathList}])";
    }

    #endregion

    #region 辅助方法

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
        int resultCount = -1;

        try
        {
            T result = await queryFunc();
            stopwatch.Stop();

            // 计算结果集大小（如果可能）
            if (result is ICollection<object> collection)
                resultCount = collection.Count;
            else if (result is int intValue)
                resultCount = 1;
            else if (result is IEnumerable<object> enumerable)
                resultCount = enumerable.Count();

            // 收集性能指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    queryType,
                    sql,
                    filesCount,
                    resultCount >= 0 ? resultCount : 0,
                    stopwatch.ElapsedMilliseconds,
                    true);

                // 检查是否是慢查询
                if (_configuration.LogSlowQueries && stopwatch.ElapsedMilliseconds > _configuration.SlowQueryThresholdMs)
                {
                    _logger.Warn($"[慢查询] {queryType}查询耗时: {stopwatch.ElapsedMilliseconds}ms, SQL: {sql}");
                }
            }

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

            // 收集失败指标
            if (_configuration.EnablePerformanceMonitoring)
            {
                QueryPerformanceMonitor.RecordQueryExecution(
                    queryType,
                    sql,
                    filesCount,
                    0,
                    stopwatch.ElapsedMilliseconds,
                    false,
                    ex);
            }

            _logger.Error($"[DuckDB指标错误] 类型: {queryType} | 文件数: {filesCount} | " +
                          $"执行时间: {stopwatch.ElapsedMilliseconds}ms | 错误: {ex.Message}");

            // 自动重试功能
            if (_configuration.AutoRetryFailedQueries && IsRetryableException(ex))
            {
                return await RetryOperationAsync(queryFunc, queryType, sql, filesCount);
            }

            throw;
        }
    }

    /// <summary>
    /// 判断异常是否可以重试
    /// </summary>
    private bool IsRetryableException(Exception ex)
    {
        // 识别可以重试的异常类型
        return ex is DuckDBException duckEx &&
               (duckEx.Message.Contains("timeout") ||
                duckEx.Message.Contains("connection") ||
                duckEx.Message.Contains("temporary"));
    }

    /// <summary>
    /// 重试操作
    /// </summary>
    private async Task<T> RetryOperationAsync<T>(
        Func<Task<T>> operation,
        string queryType,
        string sql,
        int filesCount)
    {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount < _configuration.MaxRetryCount)
        {
            retryCount++;

            try
            {
                // 等待一段时间后重试
                await Task.Delay(TimeSpan.FromMilliseconds(
                    _configuration.RetryInterval.TotalMilliseconds * Math.Pow(2, retryCount - 1))); // 指数退避

                _logger.Warn($"[DuckDB重试] 查询类型: {queryType} | 第 {retryCount} 次重试...");

                var stopwatch = Stopwatch.StartNew();
                T result = await operation();
                stopwatch.Stop();

                _logger.Info($"[DuckDB重试成功] 查询类型: {queryType} | 重试次数: {retryCount} | " +
                             $"执行时间: {stopwatch.ElapsedMilliseconds}ms");

                // 记录重试成功指标
                if (_configuration.EnablePerformanceMonitoring)
                {
                    int resultCount = -1;
                    if (result is ICollection<object> collection)
                        resultCount = collection.Count;
                    else if (result is int intValue)
                        resultCount = 1;

                    QueryPerformanceMonitor.RecordQueryExecution(
                        $"{queryType}Retry",
                        sql,
                        filesCount,
                        resultCount >= 0 ? resultCount : 0,
                        stopwatch.ElapsedMilliseconds,
                        true);
                }

                return result;
            }
            catch (Exception ex)
            {
                lastException = ex;
                _logger.Warn($"[DuckDB重试失败] 查询类型: {queryType} | 重试次数: {retryCount} | 错误: {ex.Message}");

                // 记录重试失败指标
                if (_configuration.EnablePerformanceMonitoring)
                {
                    QueryPerformanceMonitor.RecordQueryExecution(
                        $"{queryType}RetryFailed",
                        sql,
                        filesCount,
                        0,
                        0,
                        false,
                        ex);
                }

                // 如果不是可重试异常，则中断重试
                if (!IsRetryableException(ex))
                {
                    _logger.Error($"[DuckDB重试终止] 查询类型: {queryType} | 遇到不可重试的异常");
                    break;
                }
            }
        }

        // 所有重试都失败
        _logger.Error($"[DuckDB重试耗尽] 查询类型: {queryType} | 已达到最大重试次数: {_configuration.MaxRetryCount}");
        throw new Exception($"执行查询失败，已重试 {retryCount} 次", lastException);
    }

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
    /// 验证文件路径
    /// </summary>
    private void ValidateFilePaths(IEnumerable<string> filePaths)
    {
        if (filePaths == null || !filePaths.Any())
            throw new ArgumentException("必须提供至少一个文件路径", nameof(filePaths));
    }

    /// <summary>
    /// 获取列名
    /// </summary>
    private string GetColumnName<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> selector)
    {
        if (selector.Body is MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }

        throw new ArgumentException("Selector必须是一个属性访问表达式");
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

    #region IDisposable 实现

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
                    // 输出性能报告（如果已启用）
                    if (_configuration.EnablePerformanceMonitoring)
                    {
                        var report = QueryPerformanceMonitor.GenerateReport();
                        _logger.Info($"DuckDB性能报告: 总查询数: {report.TotalQueries}, " +
                                     $"成功率: {report.OverallSuccessRate:F2}%, " +
                                     $"平均耗时: {report.AverageQueryTimeMs:F2}ms");
                    }

                    // 清理预编译语句缓存
                    foreach (var statementWrapper in _statementCache.Values)
                    {
                        statementWrapper.Dispose();
                    }

                    _statementCache.Clear();

                    // 输出缓存统计
                    _logger.Info($"DuckDB元数据缓存统计: {DuckDBMetadataCache.GetStatistics()}");

                    // 释放连接
                    if (_configuration.UseConnectionPool && _pooledConnection != null)
                    {
                        // 返回连接到池
                        _connectionPool?.ReleaseConnection(_pooledConnection);
                        _pooledConnection = null;
                        _connection = null;
                        _logger.Debug("DuckDB连接已释放回连接池");
                    }
                    else if (_connection != null)
                    {
                        // 直接关闭连接
                        _connection?.Close();
                        _connection?.Dispose();
                        _connection = null;
                        _logger.Debug("DuckDB连接资源已释放");
                    }
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

// 列属性特性
public class ColumnAttribute : Attribute
{
    public string Name { get; }

    public ColumnAttribute(string name)
    {
        Name = name;
    }
}

// 未映射特性
public class NotMappedAttribute : Attribute
{
}
