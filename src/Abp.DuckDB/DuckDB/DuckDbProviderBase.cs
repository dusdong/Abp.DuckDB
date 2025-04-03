using System.Collections.Concurrent;
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
    /// <param name="performanceMonitor">性能监视器，如果为null则创建新实例</param>
    protected DuckDbProviderBase(ILogger logger, QueryPerformanceMonitor performanceMonitor = null)
    {
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
            _logger.Error("初始化DuckDB查询提供程序失败", ex);
            throw;
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
                _logger.Error($"创建预编译语句失败: {ex.Message}, SQL: {key}", ex);
                throw;
            }
        });
    }

    #endregion

    #region 通用查询执行

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

            return await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.Error($"执行自定义SQL查询失败: {ex.Message}，SQL语句：{sql}", ex);
            throw new Exception($"执行自定义SQL查询失败", ex);
        }
    }

    /// <summary>
    /// 执行读取器查询并转换为实体列表
    /// </summary>
    protected async Task<List<TEntity>> ExecuteReaderAsync<TEntity>(DuckDBCommand command)
    {
        var result = new List<TEntity>();

        using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        // 从缓存获取属性信息（优化）
        var type = typeof(TEntity);
        var properties = DuckDBMetadataCache.GetOrAddProperties(type, t =>
            t.GetProperties()
                .Where(prop => !prop.GetCustomAttributes(typeof(NotMappedAttribute), true).Any())
                .ToArray());

        // 缓存列索引以提高性能
        var columnMappings = BuildColumnMappings(reader, properties);

        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var entity = MapReaderToEntity<TEntity>(reader, properties, columnMappings);
            result.Add(entity);
        }

        return result;
    }

    /// <summary>
    /// 构建列映射字典
    /// </summary>
    protected Dictionary<PropertyInfo, int> BuildColumnMappings(
        System.Data.Common.DbDataReader reader,
        PropertyInfo[] properties)
    {
        // 构建读取器列名到索引的映射
        var columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnIndexMap[reader.GetName(i)] = i;
        }

        // 构建属性到列索引的映射
        var result = new Dictionary<PropertyInfo, int>();
        foreach (var property in properties)
        {
            // 获取列名（考虑特性标记）
            string columnName = GetColumnNameFromProperty(property);

            // 查找匹配的列索引
            if (columnIndexMap.TryGetValue(columnName, out int columnIndex))
            {
                result[property] = columnIndex;
            }
        }

        return result;
    }

    /// <summary>
    /// 从属性获取列名
    /// </summary>
    protected string GetColumnNameFromProperty(PropertyInfo property)
    {
        // 这里可以扩展以支持自定义列名特性
        // 例如 [Column("custom_name")]
        return property.Name;
    }

    /// <summary>
    /// 将读取器行映射为实体
    /// </summary>
    protected TEntity MapReaderToEntity<TEntity>(
        System.Data.Common.DbDataReader reader,
        PropertyInfo[] properties,
        Dictionary<PropertyInfo, int> columnMappings)
    {
        TEntity entity = Activator.CreateInstance<TEntity>();

        foreach (var property in properties)
        {
            // 只处理包含在映射中的属性
            if (columnMappings.TryGetValue(property, out int columnIndex))
            {
                if (!reader.IsDBNull(columnIndex))
                {
                    try
                    {
                        var value = reader.GetValue(columnIndex);
                        ConvertAndSetValue(entity, property, value);
                    }
                    catch (Exception ex)
                    {
                        _logger.Warn($"转换属性 {property.Name} 失败: {ex.Message}");
                    }
                }
            }
        }

        return entity;
    }

    /// <summary>
    /// 转换数据库值并设置属性
    /// </summary>
    private void ConvertAndSetValue<TEntity>(TEntity entity, PropertyInfo property, object value)
    {
        try
        {
            if (value == DBNull.Value)
                return; // 保持属性默认值

            var targetType = property.PropertyType;

            // 处理可空类型
            var nullableType = Nullable.GetUnderlyingType(targetType);
            if (nullableType != null)
            {
                targetType = nullableType;
            }

            // 处理枚举类型
            if (targetType.IsEnum)
            {
                if (value is string strValue)
                {
                    property.SetValue(entity, Enum.Parse(targetType, strValue));
                }
                else
                {
                    property.SetValue(entity, Enum.ToObject(targetType, value));
                }

                return;
            }

            // 处理常见类型转换
            if (value is decimal decValue && targetType == typeof(double))
            {
                property.SetValue(entity, Convert.ToDouble(decValue));
                return;
            }

            if (value is long longValue && targetType == typeof(int))
            {
                property.SetValue(entity, Convert.ToInt32(longValue));
                return;
            }

            // 通用转换
            property.SetValue(entity, Convert.ChangeType(value, targetType));
        }
        catch (Exception ex)
        {
            _logger.Error($"设置属性 {property.Name} 值时发生异常: {ex.Message}", ex);
            throw;
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

    #region 表达式处理辅助方法

    /// <summary>
    /// 从表达式中提取属性名
    /// </summary>
    protected string GetColumnName<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> selector)
    {
        if (selector.Body is MemberExpression memberExpression)
        {
            return memberExpression.Member.Name;
        }
        else if (selector.Body is UnaryExpression unaryExpression &&
                 unaryExpression.Operand is MemberExpression operandMemberExpression)
        {
            return operandMemberExpression.Member.Name;
        }

        throw new ArgumentException("表达式必须是属性访问表达式", nameof(selector));
    }

    /// <summary>
    /// 获取实体的列名列表
    /// </summary>
    protected IEnumerable<string> GetEntityColumns<TEntity>()
    {
        // 优先从缓存获取列名
        var type = typeof(TEntity);
        return DuckDBMetadataCache.GetOrAddEntityColumns(type, t =>
        {
            // 排除未映射属性
            var properties = t.GetProperties()
                .Where(p => !p.GetCustomAttributes(typeof(NotMappedAttribute), true).Any());

            // 为每个属性获取列名
            return properties.Select(p => GetColumnNameFromProperty(p)).ToList();
        });
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
