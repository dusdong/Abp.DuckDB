using System.Collections.Concurrent;
using System.Data;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.MultiTenant;

/// <summary>
/// DuckDB连接池，为多租户系统提供连接管理功能
/// </summary>
public class DuckDBConnectionPool : IDisposable
{
    private readonly ConcurrentDictionary<string, PooledConnectionQueue> _connectionPools = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _poolLocks = new();
    private readonly TenantDatabaseManager _dbManager;
    private readonly ILogger _logger;
    private readonly Timer _maintenanceTimer;
    private bool _disposed;

    /// <summary>
    /// 每个租户池的最大连接数
    /// </summary>
    public int MaxPoolSize { get; set; } = 10;

    /// <summary>
    /// 连接获取超时（毫秒）
    /// </summary>
    public int ConnectionTimeout { get; set; } = 30000;

    /// <summary>
    /// 连接最大空闲时间（秒）
    /// </summary>
    public int MaxIdleTimeSeconds { get; set; } = 300;

    /// <summary>
    /// 连接检查间隔（秒）
    /// </summary>
    public int MaintenanceIntervalSeconds { get; set; } = 60;

    /// <summary>
    /// 创建DuckDB连接池
    /// </summary>
    /// <param name="dbManager">租户数据库管理器</param>
    /// <param name="logger">日志记录器</param>
    public DuckDBConnectionPool(TenantDatabaseManager dbManager, ILogger logger)
    {
        _dbManager = dbManager ?? throw new ArgumentNullException(nameof(dbManager));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _maintenanceTimer = new Timer(MaintenanceCallback, null, TimeSpan.FromSeconds(MaintenanceIntervalSeconds), TimeSpan.FromSeconds(MaintenanceIntervalSeconds));
    }

    /// <summary>
    /// 获取连接
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>来自连接池的DuckDB连接</returns>
    public async Task<PooledConnection> GetConnectionAsync(string tenantId)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DuckDBConnectionPool));

        var poolLock = _poolLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));

        try
        {
            await poolLock.WaitAsync(ConnectionTimeout).ConfigureAwait(false);

            var pool = _connectionPools.GetOrAdd(tenantId, _ => new PooledConnectionQueue(MaxPoolSize));
            var connectionString = _dbManager.GetConnectionString(tenantId);

            // 尝试获取可用连接
            PooledConnection connection = null;
            while (pool.Count > 0)
            {
                if (pool.TryDequeue(out connection))
                {
                    // 验证连接状态
                    if (connection?.IsValid == true)
                    {
                        return connection;
                    }
                    else
                    {
                        // 处理失效连接
                        connection?.Dispose();
                    }
                }
            }

            // 创建新连接
            if (pool.CurrentCount < MaxPoolSize)
            {
                try
                {
                    var newConnection = new DuckDBConnection(connectionString);
                    newConnection.Open();

                    var pooledConnection = new PooledConnection(newConnection, tenantId, this);
                    _logger.Debug($"为租户 {tenantId} 创建新连接，当前池大小: {pool.CurrentCount + 1}");

                    Interlocked.Increment(ref pool.CurrentCount);
                    return pooledConnection;
                }
                catch (Exception ex)
                {
                    _logger.Error($"为租户 {tenantId} 创建新连接失败: {ex.Message}", ex);
                    throw;
                }
            }
            else
            {
                _logger.Warn($"租户 {tenantId} 的连接池已达最大值 {MaxPoolSize}，等待可用连接");

                // 池已满，等待可用连接
                var waitTask = Task.Delay(ConnectionTimeout);
                var dequeueTryCount = 0;

                while (!waitTask.IsCompleted)
                {
                    if (pool.TryDequeue(out connection))
                    {
                        if (connection?.IsValid == true)
                        {
                            return connection;
                        }
                        else
                        {
                            // 处理失效连接
                            connection?.Dispose();
                            Interlocked.Decrement(ref pool.CurrentCount);
                            // 再次尝试创建新连接
                            return await GetConnectionAsync(tenantId).ConfigureAwait(false);
                        }
                    }

                    dequeueTryCount++;
                    if (dequeueTryCount > 5)
                    {
                        await Task.Delay(100).ConfigureAwait(false);
                        dequeueTryCount = 0;
                    }
                }

                throw new TimeoutException($"无法获取租户 {tenantId} 的数据库连接，连接池已满且超时");
            }
        }
        finally
        {
            poolLock.Release();
        }
    }

    /// <summary>
    /// 将连接返回池中
    /// </summary>
    internal void ReturnConnection(PooledConnection connection)
    {
        if (_disposed || connection == null)
            return;

        var tenantId = connection.TenantId;

        if (_connectionPools.TryGetValue(tenantId, out var pool))
        {
            if (connection.IsValid)
            {
                connection.LastUsedTime = DateTime.UtcNow;
                pool.Enqueue(connection);
                _logger.Debug($"将连接返回租户 {tenantId} 的连接池");
            }
            else
            {
                // 处理失效连接
                connection.Dispose();
                Interlocked.Decrement(ref pool.CurrentCount);
                _logger.Debug($"销毁租户 {tenantId} 的失效连接，当前池大小: {pool.CurrentCount}");
            }
        }
        else
        {
            // 如果池不存在，直接释放连接
            connection.Dispose();
            _logger.Debug($"租户 {tenantId} 的连接池不存在，直接释放连接");
        }
    }

    /// <summary>
    /// 获取租户的连接池统计信息
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>连接池信息</returns>
    public ConnectionPoolInfo GetPoolInfo(string tenantId)
    {
        if (_connectionPools.TryGetValue(tenantId, out var pool))
        {
            return new ConnectionPoolInfo
            {
                TenantId = tenantId,
                TotalConnections = pool.CurrentCount,
                AvailableConnections = pool.Count
            };
        }

        return new ConnectionPoolInfo
        {
            TenantId = tenantId,
            TotalConnections = 0,
            AvailableConnections = 0
        };
    }

    /// <summary>
    /// 清除租户的连接池
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    public async Task ClearPoolAsync(string tenantId)
    {
        if (_poolLocks.TryGetValue(tenantId, out var poolLock))
        {
            await poolLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_connectionPools.TryRemove(tenantId, out var pool))
                {
                    _logger.Info($"清除租户 {tenantId} 的连接池");

                    while (pool.TryDequeue(out var connection))
                    {
                        connection?.Dispose();
                        Interlocked.Decrement(ref pool.CurrentCount);
                    }
                }
            }
            finally
            {
                poolLock.Release();
            }
        }
    }

    /// <summary>
    /// 连接池维护回调，清理空闲连接
    /// </summary>
    private async void MaintenanceCallback(object state)
    {
        if (_disposed)
            return;

        try
        {
            _logger.Debug("开始连接池维护检查...");
            var idleTimeout = DateTime.UtcNow.AddSeconds(-MaxIdleTimeSeconds);

            foreach (var kvp in _connectionPools)
            {
                var tenantId = kvp.Key;
                var pool = kvp.Value;

                if (_poolLocks.TryGetValue(tenantId, out var poolLock))
                {
                    if (await poolLock.WaitAsync(0).ConfigureAwait(false))
                    {
                        try
                        {
                            int removedCount = 0;

                            // 移除空闲连接，同时保留至少一个连接在池中
                            while (pool.Count > 1)
                            {
                                if (pool.TryPeek(out var connection) && connection.LastUsedTime < idleTimeout)
                                {
                                    if (pool.TryDequeue(out connection))
                                    {
                                        connection.Dispose();
                                        Interlocked.Decrement(ref pool.CurrentCount);
                                        removedCount++;
                                    }
                                }
                                else
                                {
                                    break;
                                }
                            }

                            if (removedCount > 0)
                            {
                                _logger.Debug($"从租户 {tenantId} 的连接池中移除了 {removedCount} 个空闲连接");
                            }
                        }
                        finally
                        {
                            poolLock.Release();
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"连接池维护过程中发生错误: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 释放所有连接和资源
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            _maintenanceTimer?.Dispose();

            foreach (var kvp in _connectionPools)
            {
                var pool = kvp.Value;

                while (pool.TryDequeue(out var connection))
                {
                    connection?.Dispose();
                }
            }

            _connectionPools.Clear();

            foreach (var kvp in _poolLocks)
            {
                kvp.Value?.Dispose();
            }

            _poolLocks.Clear();
        }
        catch (Exception ex)
        {
            _logger.Error($"连接池释放资源时发生错误: {ex.Message}", ex);
        }
    }
}

/// <summary>
/// 池化连接包装器
/// </summary>
public class PooledConnection : IDisposable
{
    private readonly DuckDBConnection _connection;
    private readonly DuckDBConnectionPool _pool;
    private bool _disposed;

    /// <summary>
    /// 租户ID
    /// </summary>
    public string TenantId { get; }

    /// <summary>
    /// 最后使用时间
    /// </summary>
    public DateTime LastUsedTime { get; set; }

    /// <summary>
    /// 连接是否有效
    /// </summary>
    public bool IsValid => !_disposed && _connection?.State == ConnectionState.Open;

    internal PooledConnection(DuckDBConnection connection, string tenantId, DuckDBConnectionPool pool)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        LastUsedTime = DateTime.UtcNow;
    }

    /// <summary>
    /// 获取内部DuckDB连接
    /// </summary>
    public DuckDBConnection Connection
    {
        get
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(PooledConnection));
            LastUsedTime = DateTime.UtcNow;
            return _connection;
        }
    }

    /// <summary>
    /// 创建命令对象
    /// </summary>
    /// <returns>DuckDB命令</returns>
    public DuckDBCommand CreateCommand()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PooledConnection));

        LastUsedTime = DateTime.UtcNow;
        return _connection.CreateCommand();
    }

    /// <summary>
    /// 开始事务
    /// </summary>
    /// <returns>DuckDB事务</returns>
    public DuckDBTransaction BeginTransaction()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PooledConnection));

        LastUsedTime = DateTime.UtcNow;
        return _connection.BeginTransaction();
    }

    /// <summary>
    /// 释放连接，将其返回池中
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _pool.ReturnConnection(this);
        }
    }
}

/// <summary>
/// 池化连接队列
/// </summary>
internal class PooledConnectionQueue : ConcurrentQueue<PooledConnection>
{
    public int MaxSize { get; }
    public int CurrentCount;

    public PooledConnectionQueue(int maxSize)
    {
        MaxSize = maxSize;
        CurrentCount = 0;
    }
}

/// <summary>
/// 连接池统计信息
/// </summary>
public class ConnectionPoolInfo
{
    public string TenantId { get; set; }
    public int TotalConnections { get; set; }
    public int AvailableConnections { get; set; }
    public int BusyConnections => TotalConnections - AvailableConnections;
}
