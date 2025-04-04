using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB;

/// <summary>
/// DuckDB查询提供程序基础实现
/// </summary>
public abstract class DuckDbProviderBase : IDuckDBProvider
{
    #region 私有字段与内部类

    // 连接池相关 - 改为实例字段以支持注入
    private DuckDBConnectionPool _connectionPool;
    private readonly object _poolInitLock = new object();
    private Timer _maintenanceTimer;

    // 性能监控相关 - 改为实例字段
    protected readonly QueryPerformanceMonitor _performanceMonitor = new QueryPerformanceMonitor();

    // 连接相关
    protected DuckDBConnection _connection;
    protected readonly ILogger _logger;
    protected bool _disposed = false;
    protected DuckDBConfiguration _configuration;
    protected PooledConnection _pooledConnection;

    // 添加预编译语句缓存
    protected readonly ConcurrentDictionary<string, PreparedStatementWrapper> _statementCache = new();

    // DuckDB SQL构建器
    protected readonly DuckDBSqlBuilder _sqlBuilder;

    /// <summary>
    /// 预编译语句包装器，用于管理预编译语句的生命周期
    /// </summary>
    protected class PreparedStatementWrapper : IDisposable
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

    #region 构造函数

    /// <summary>
    /// 构造DuckDB查询提供程序
    /// </summary>
    /// <param name="logger">日志记录器</param>
    /// <param name="sqlBuilder">SQL构建器</param>
    /// <param name="performanceMonitor">性能监视器，如果为null则创建新实例</param>
    protected DuckDbProviderBase(ILogger logger, DuckDBSqlBuilder sqlBuilder, QueryPerformanceMonitor performanceMonitor)
    {
        _sqlBuilder = sqlBuilder;
        _logger = logger ?? NullLogger.Instance;
        _configuration = DuckDBConfiguration.HighPerformance(); // 使用默认配置
        _performanceMonitor = performanceMonitor ?? new QueryPerformanceMonitor();
    }

    #endregion

    #region 连接池管理

    /// <summary>
    /// 设置外部连接池实例
    /// </summary>
    /// <param name="connectionPool">要使用的连接池实例</param>
    public void SetConnectionPool(DuckDBConnectionPool connectionPool)
    {
        if (connectionPool == null)
            throw new ArgumentNullException(nameof(connectionPool));

        lock (_poolInitLock)
        {
            // 如果已有连接池，先停止维护计时器
            if (_maintenanceTimer != null)
            {
                _maintenanceTimer.Dispose();
                _maintenanceTimer = null;
            }

            _connectionPool = connectionPool;
            _logger.Info("已设置外部DuckDB连接池实例");

            // 为新连接池启动维护计时器
            StartPoolMaintenanceTimer();
        }
    }

    /// <summary>
    /// 启动连接池维护计时器
    /// </summary>
    private void StartPoolMaintenanceTimer()
    {
        if (_connectionPool != null && _maintenanceTimer == null)
        {
            _maintenanceTimer = new Timer(
                _ => PerformPoolMaintenance(),
                null,
                TimeSpan.FromMinutes(5),
                TimeSpan.FromMinutes(5));
        }
    }

    /// <summary>
    /// 执行连接池维护
    /// </summary>
    private void PerformPoolMaintenance()
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
                StartPoolMaintenanceTimer();
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
            HandleException("初始化DuckDB查询提供程序", ex);
        }
    }

    /// <summary>
    /// 初始化连接池
    /// </summary>
    protected void InitializeConnectionPool()
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

    #endregion

    #region 语句缓存

    /// <summary>
    /// 获取或创建预编译语句
    /// </summary>
    protected PreparedStatementWrapper GetOrCreatePreparedStatement(string sql, bool forceCreate = false)
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
                HandleException("创建预编译语句", ex, key);
                throw; // 这里不会执行到，因为HandleException会抛出异常
            }
        });
    }

    #endregion

    #region 通用查询构建方法

    /// <summary>
    /// 构建查询SQL
    /// </summary>
    protected string BuildSelectQuery(
        string dataSource,
        string whereClause,
        IEnumerable<string> selectedColumns = null)
    {
        string columns = selectedColumns != null && selectedColumns.Any()
            ? string.Join(", ", selectedColumns)
            : "*";

        var sql = $"SELECT {columns} FROM {dataSource}";

        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        return sql;
    }

    /// <summary>
    /// 构建COUNT查询
    /// </summary>
    protected string BuildCountQuery(string dataSource, string whereClause)
    {
        var sql = $"SELECT COUNT(*) FROM {dataSource}";

        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        return sql;
    }

    /// <summary>
    /// 执行分页查询
    /// </summary>
    protected async Task<(List<TEntity> Items, int TotalCount)> ExecutePagedQueryAsync<TEntity>(
        string dataSource,
        string whereClause,
        int pageIndex,
        int pageSize,
        string orderByColumn = null,
        bool ascending = true,
        IEnumerable<string> selectedColumns = null,
        CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        if (pageIndex < 0) throw new ArgumentException("页码必须大于或等于0", nameof(pageIndex));
        if (pageSize <= 0) throw new ArgumentException("页大小必须大于0", nameof(pageSize));

        return await ExecuteSafelyAsync(
            async () =>
            {
                // 计算分页参数
                int offset = pageIndex * pageSize;

                // 构建分页查询
                string columns = selectedColumns != null && selectedColumns.Any()
                    ? string.Join(", ", selectedColumns)
                    : "*";

                var querySql = $"SELECT {columns} FROM {dataSource}";
                if (!string.IsNullOrEmpty(whereClause))
                {
                    querySql += $" WHERE {whereClause}";
                }

                if (!string.IsNullOrEmpty(orderByColumn))
                {
                    querySql += $" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}";
                }

                querySql += $" LIMIT {pageSize} OFFSET {offset}";

                // 构建计数查询
                var countSql = $"SELECT COUNT(*) FROM {dataSource}";
                if (!string.IsNullOrEmpty(whereClause))
                {
                    countSql += $" WHERE {whereClause}";
                }

                // 并行执行两个查询以提高性能
                var itemsTask = ExecuteQueryWithMetricsAsync(
                    () => QueryWithRawSqlAsync<TEntity>(querySql, cancellationToken),
                    "PagedQuery",
                    querySql);

                var countTask = ExecuteQueryWithMetricsAsync(
                    async () =>
                    {
                        using var cmd = _connection.CreateCommand();
                        cmd.CommandText = countSql;

                        if (_configuration.CommandTimeout != TimeSpan.Zero)
                            cmd.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

                        var result = await cmd.ExecuteScalarAsync(cancellationToken);
                        return DuckDBTypeConverter.SafeConvert<int>(result, _logger);
                    },
                    "CountQuery",
                    countSql);

                // 等待两个任务完成
                await Task.WhenAll(itemsTask, countTask);

                // 返回结果
                return (await itemsTask, await countTask);
            },
            "执行分页查询",
            $"数据源: {dataSource}, 页码: {pageIndex}, 每页大小: {pageSize}"
        );
    }

    /// <summary>
    /// 流式查询处理大量数据
    /// </summary>
    protected async Task<int> QueryStreamInternalAsync<TEntity>(
        string dataSource,
        string whereClause,
        int batchSize,
        Func<IEnumerable<TEntity>, Task> processAction,
        CancellationToken cancellationToken = default)
    {
        var sql = BuildSelectQuery(dataSource, whereClause);
        int totalProcessed = 0;

        using var command = _connection.CreateCommand();
        command.CommandText = sql;

        if (_configuration.CommandTimeout != TimeSpan.Zero)
            command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var properties = GetEntityProperties<TEntity>();
        var columnMappings = GetColumnMappings(reader, properties);

        var batch = new List<TEntity>(batchSize);

        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
            batch.Add(entity);
            totalProcessed++;

            if (batch.Count >= batchSize)
            {
                if (processAction != null)
                    await processAction(batch);

                batch.Clear();
            }
        }

        // 处理最后一批数据
        if (batch.Count > 0 && processAction != null)
        {
            await processAction(batch);
        }

        return totalProcessed;
    }

    #endregion

    #region 缓存和反射优化 - 使用统一缓存

    /// <summary>
    /// 获取实体属性（使用缓存）
    /// </summary>
    protected PropertyInfo[] GetEntityProperties<TEntity>()
    {
        return DuckDBMetadataCache.GetOrAddProperties(
            typeof(TEntity),
            type => type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
        );
    }

    /// <summary>
    /// 获取实体列名（使用缓存）
    /// </summary>
    protected string[] GetEntityColumns<TEntity>()
    {
        var columns = DuckDBMetadataCache.GetOrAddEntityColumns(
            typeof(TEntity),
            type =>
            {
                var properties = GetEntityProperties<TEntity>();
                return properties.Select(p => p.Name).ToList();
            }
        );

        return columns.ToArray();
    }

    /// <summary>
    /// 从表达式获取列名
    /// </summary>
    protected string GetColumnName<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> selector)
    {
        // 缓存键
        string cacheKey = $"{typeof(TEntity).FullName}_{selector}";

        return DuckDBMetadataCache.GetOrAddExpressionSql(
            cacheKey,
            _ =>
            {
                // 简单成员表达式直接提取名称
                if (selector.Body is MemberExpression memberExp)
                {
                    return memberExp.Member.Name;
                }

                // 处理转换表达式
                if (selector.Body is UnaryExpression unaryExp &&
                    unaryExp.Operand is MemberExpression memberOperand)
                {
                    return memberOperand.Member.Name;
                }

                throw new ArgumentException($"无法从表达式解析列名: {selector}", nameof(selector));
            }
        );
    }

    /// <summary>
    /// 统一处理异常
    /// </summary>
    /// <param name="operation">操作名称</param>
    /// <param name="ex">异常</param>
    /// <param name="sql">相关SQL语句（可选）</param>
    /// <param name="context">上下文信息（可选）</param>
    /// <param name="shouldThrow">是否应该抛出异常</param>
    protected void HandleException(string operation, Exception ex, string sql = null, string context = null, bool shouldThrow = true)
    {
        string message = $"{operation}失败: {ex.Message}";
        if (!string.IsNullOrEmpty(context))
            message += $", {context}";

        _logger.Error(message, ex);

        if (shouldThrow)
        {
            if (sql != null)
                throw new DuckDBOperationException(operation, sql, message, ex);
            else
                throw new DuckDBOperationException(operation, message, ex);
        }
    }

    /// <summary>
    /// 获取数据库列名到实体属性的映射
    /// </summary>
    /// <param name="reader">数据库读取器</param>
    /// <param name="properties">实体属性数组</param>
    /// <returns>列名到属性索引的映射字典</returns>
    protected Dictionary<string, int> GetColumnMappings(DbDataReader reader, PropertyInfo[] properties)
    {
        var columnMappings = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        // 创建字段名到索引的映射
        for (int i = 0; i < reader.FieldCount; i++)
        {
            string columnName = reader.GetName(i);

            // 查找匹配的属性
            for (int j = 0; j < properties.Length; j++)
            {
                // 检查精确匹配
                if (string.Equals(properties[j].Name, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    columnMappings[columnName] = j;
                    break;
                }
            }
        }

        return columnMappings;
    }

    /// <summary>
    /// 将数据库读取器数据映射到实体
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="reader">数据库读取器</param>
    /// <param name="properties">实体属性数组</param>
    /// <param name="columnMappings">列名到属性索引的映射</param>
    /// <returns>映射后的实体对象</returns>
    protected TEntity MapReaderToEntity<TEntity>(
        DbDataReader reader,
        PropertyInfo[] properties,
        Dictionary<string, int> columnMappings)
    {
        // 创建实体实例
        var entity = Activator.CreateInstance<TEntity>();

        // 遍历所有列并设置属性值
        for (int i = 0; i < reader.FieldCount; i++)
        {
            string columnName = reader.GetName(i);

            // 检查是否有对应的属性映射
            if (columnMappings.TryGetValue(columnName, out int propertyIndex))
            {
                var property = properties[propertyIndex];
                object value = reader.IsDBNull(i) ? null : reader.GetValue(i);

                if (value != null || property.PropertyType.IsNullable())
                {
                    try
                    {
                        // 将值转换为属性类型并设置
                        var convertedValue = ConvertValueToPropertyType(value, property.PropertyType);
                        property.SetValue(entity, convertedValue);
                    }
                    catch (Exception ex)
                    {
                        // 记录错误但继续处理，避免单个属性问题导致整个实体失败
                        _logger.Warn($"设置属性 {property.Name} 值失败: {ex.Message}", ex);
                    }
                }
            }
        }

        return entity;
    }

    /// <summary>
    /// 将值转换为指定的属性类型
    /// </summary>
    /// <param name="value">原始值</param>
    /// <param name="targetType">目标类型</param>
    /// <returns>转换后的值</returns>
    protected object ConvertValueToPropertyType(object value, Type targetType)
    {
        if (value == null || value == DBNull.Value)
        {
            return null;
        }

        return DuckDBTypeConverter.SafeConvert(value, targetType, _logger);
    }

    /// <summary>
    /// 使用原始SQL执行查询
    /// </summary>
    protected async Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, CancellationToken cancellationToken = default)
    {
        var results = new List<TEntity>();

        using var command = _connection.CreateCommand();
        command.CommandText = sql;

        if (_configuration.CommandTimeout != TimeSpan.Zero)
            command.CommandTimeout = (int)_configuration.CommandTimeout.TotalSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var properties = GetEntityProperties<TEntity>();
        var columnMappings = GetColumnMappings(reader, properties);

        while (await reader.ReadAsync(cancellationToken))
        {
            var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
            results.Add(entity);
        }

        return results;
    }

    /// <summary>
    /// 获取列名从属性
    /// </summary>
    protected string GetColumnNameFromProperty(PropertyInfo property)
    {
        // 这里可以扩展以支持自定义列名特性
        // 例如 [Column("custom_name")]
        return property.Name;
    }

    #endregion

    #region 通用查询执行

    /// <summary>
    /// 安全执行操作，统一处理异常
    /// </summary>
    /// <typeparam name="T">返回类型</typeparam>
    /// <param name="action">要执行的操作</param>
    /// <param name="operationName">操作名称，用于日志和错误消息</param>
    /// <param name="context">上下文信息，可选</param>
    /// <param name="rethrowWrapped">是否将异常包装后重新抛出，默认为true</param>
    /// <returns>操作结果</returns>
    protected async Task<T> ExecuteSafelyAsync<T>(
        Func<Task<T>> action,
        string operationName,
        string context = null,
        bool rethrowWrapped = true)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        try
        {
            return await action();
        }
        catch (Exception ex)
        {
            HandleException(operationName, ex, null, context, rethrowWrapped);
            return default; // 这里不会执行到，因为HandleException会抛出异常
        }
    }

    /// <summary>
    /// 安全执行无返回值操作，统一处理异常
    /// </summary>
    /// <param name="action">要执行的操作</param>
    /// <param name="operationName">操作名称，用于日志和错误消息</param>
    /// <param name="context">上下文信息，可选</param>
    /// <param name="rethrowWrapped">是否将异常包装后重新抛出，默认为true</param>
    protected async Task ExecuteSafelyAsync(
        Func<Task> action,
        string operationName,
        string context = null,
        bool rethrowWrapped = true)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        try
        {
            await action();
        }
        catch (Exception ex)
        {
            HandleException(operationName, ex, null, context, rethrowWrapped);
        }
    }

    /// <summary>
    /// 执行聚合查询，用于SUM, AVG, MIN, MAX等操作
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <typeparam name="TResult">聚合结果类型</typeparam>
    /// <param name="aggregateFunction">聚合函数名称 (SUM, AVG, MIN, MAX等)</param>
    /// <param name="columnName">要聚合的列名</param>
    /// <param name="whereClause">WHERE子句(不包含WHERE关键字)</param>
    /// <param name="dataSource">数据源SQL片段</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>聚合结果</returns>
    protected async Task<TResult> ExecuteAggregateAsync<TEntity, TResult>(
        string aggregateFunction,
        string columnName,
        string whereClause,
        string dataSource,
        CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        // 构建聚合函数表达式
        string aggregateExpression = $"{aggregateFunction}({columnName})";

        // 构建SQL
        var sql = $"SELECT {aggregateExpression} FROM {dataSource}";
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

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
            $"{aggregateFunction}Query",
            sql);
    }

    /// <summary>
    /// 执行聚合查询，基于表达式选择器
    /// </summary>
    protected async Task<TResult> ExecuteAggregateAsync<TEntity, TResult, TProperty>(
        string aggregateFunction,
        Expression<Func<TEntity, TProperty>> selector,
        string whereClause,
        string dataSource,
        CancellationToken cancellationToken = default)
    {
        // 从表达式提取列名
        string columnName = GetColumnName(selector);
        return await ExecuteAggregateAsync<TEntity, TResult>(
            aggregateFunction,
            columnName,
            whereClause,
            dataSource,
            cancellationToken);
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

            using var reader = await command.ExecuteReaderAsync();
            var results = new List<TEntity>();
            var properties = GetEntityProperties<TEntity>();
            var columnMappings = GetColumnMappings(reader, properties);

            while (await reader.ReadAsync())
            {
                var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
                results.Add(entity);
            }

            return results;
        }
        catch (Exception ex)
        {
            HandleException("执行自定义SQL查询", ex, sql);
            return new List<TEntity>(); // 这里不会执行到
        }
    }

    /// <summary>
    /// 通用方法：使用性能监控包装执行查询
    /// </summary>
    protected async Task<T> ExecuteQueryWithMetricsAsync<T>(
        Func<Task<T>> queryFunc,
        string queryType,
        string sql,
        int fileCount = 0)
    {
        if (!_configuration.EnablePerformanceMonitoring)
            return await queryFunc();

        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = await queryFunc();
            stopwatch.Stop();

            // 计算结果数量（如果适用）
            int resultCount = GetResultCount(result);

            // 记录查询执行
            _performanceMonitor.RecordQueryExecution(
                queryType,
                sql,
                fileCount,
                resultCount,
                stopwatch.ElapsedMilliseconds,
                true);

            // 检查是否是慢查询
            if (_configuration.LogSlowQueries && stopwatch.ElapsedMilliseconds > _configuration.SlowQueryThresholdMs)
            {
                _logger.Warn($"[慢查询] {queryType} 查询耗时: {stopwatch.ElapsedMilliseconds}ms, SQL: {sql}");
            }

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            // 记录失败的查询
            _performanceMonitor.RecordQueryExecution(
                queryType,
                sql,
                fileCount,
                0,
                stopwatch.ElapsedMilliseconds,
                false,
                ex);

            throw;
        }
    }

    /// <summary>
    /// 获取结果数量（根据返回类型）
    /// </summary>
    private int GetResultCount<T>(T result)
    {
        if (result is ICollection<object> collection)
            return collection.Count;

        if (result is ValueTuple<ICollection<object>, int> valueTuple)
            return valueTuple.Item1.Count;

        return 1; // 默认值为1，表示有结果
    }

    #endregion

    #region 缓存管理

    /// <summary>
    /// 预热实体元数据，提前缓存以提高性能
    /// </summary>
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
    public string GetCacheStatistics()
    {
        return DuckDBMetadataCache.GetStatistics();
    }

    #endregion

    #region IDuckDBPerformanceMonitor 接口实现

    /// <summary>
    /// 记录批处理进度
    /// </summary>
    public void RecordBatchProcessing(int itemCount)
    {
        // 使用现有的性能监控器方法记录批处理
        _performanceMonitor.RecordQueryExecution(
            "BatchProcessing",
            "Streaming batch processed",
            0, // 文件数量
            itemCount, // 记录数量
            0, // 执行时间 - 这里我们只关心记录数
            true); // 成功标志
    }

    /// <summary>
    /// 分析查询计划
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
    /// 获取性能报告
    /// </summary>
    public QueryPerformanceReport GetPerformanceReport()
    {
        return _performanceMonitor.GenerateReport();
    }

    /// <summary>
    /// 获取查询类型的性能指标
    /// </summary>
    public QueryPerformanceMetrics GetMetricsForQueryType(string queryType)
    {
        return _performanceMonitor.GetMetricsForQueryType(queryType);
    }

    /// <summary>
    /// 重置性能指标
    /// </summary>
    public void ResetPerformanceMetrics()
    {
        _performanceMonitor.ResetMetrics();
    }

    /// <summary>
    /// 获取最近的查询执行日志
    /// </summary>
    public List<QueryExecutionLog> GetRecentExecutions(int count = 100)
    {
        return _performanceMonitor.GetRecentExecutions(count);
    }

    /// <summary>
    /// 清除最近的执行日志
    /// </summary>
    public void ClearExecutionLogs()
    {
        _performanceMonitor.ClearExecutionLogs();
    }

    #endregion

    #region IDisposable实现

    /// <summary>
    /// 释放资源
    /// </summary>
    public virtual void Dispose()
    {
        if (!_disposed)
        {
            // 清理维护计时器
            if (_maintenanceTimer != null)
            {
                _maintenanceTimer.Dispose();
                _maintenanceTimer = null;
            }

            // 清理语句缓存
            foreach (var statement in _statementCache.Values)
            {
                statement.Dispose();
            }

            _statementCache.Clear();

            // 归还连接池连接或关闭连接
            if (_pooledConnection != null)
            {
                _pooledConnection.Release();
                _pooledConnection = null;
            }
            else if (_connection != null)
            {
                _connection.Close();
                _connection.Dispose();
                _connection = null;
            }

            _disposed = true;
        }
    }

    #endregion
}
