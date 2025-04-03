using System.Collections.Concurrent;
using System.Data;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB连接池管理器
/// </summary>
public class DuckDBConnectionPool : IDisposable
{
    private readonly ConcurrentBag<PooledConnection> _availableConnections = new();
    private readonly ConcurrentDictionary<string, PooledConnection> _busyConnections = new();
    private readonly SemaphoreSlim _poolLock = new SemaphoreSlim(1, 1);
    private readonly ILogger _logger;
    private readonly DuckDBPoolOptions _options;
    private int _totalConnections = 0;
    private bool _disposed;

    public DuckDBConnectionPool(DuckDBPoolOptions options, ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? NullLogger.Instance;

        // 预创建最小连接数
        for (int i = 0; i < options.MinConnections; i++)
        {
            try
            {
                var connection = CreateNewConnection();
                _availableConnections.Add(connection);
            }
            catch (Exception ex)
            {
                _logger.Error($"创建初始连接失败: {ex.Message}", ex);
            }
        }

        _logger.Info($"DuckDB连接池创建成功 - 最小连接数: {options.MinConnections}, 最大连接数: {options.MaxConnections}");
    }

    /// <summary>
    /// 获取连接
    /// </summary>
    public async Task<PooledConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        PooledConnection connection = null;

        // 尝试从可用连接获取
        if (_availableConnections.TryTake(out connection))
        {
            // 检查连接是否可用
            if (!IsConnectionValid(connection))
            {
                SafeCloseConnection(connection);
                connection = null;
            }
        }

        // 没有可用连接，创建新连接
        if (connection == null)
        {
            await _poolLock.WaitAsync(cancellationToken);
            try
            {
                // 检查是否达到最大连接数
                if (_totalConnections >= _options.MaxConnections)
                {
                    // 等待一段时间，尝试再次获取可用连接
                    _poolLock.Release();
                    await Task.Delay(_options.ConnectionWaitTime, cancellationToken);

                    if (_availableConnections.TryTake(out connection))
                    {
                        if (!IsConnectionValid(connection))
                        {
                            SafeCloseConnection(connection);
                            connection = null;
                        }
                    }

                    if (connection == null)
                    {
                        throw new InvalidOperationException($"连接池已满({_options.MaxConnections}个连接)，无法创建新连接");
                    }
                }
                else
                {
                    // 创建新连接
                    connection = CreateNewConnection();
                    Interlocked.Increment(ref _totalConnections);
                }
            }
            finally
            {
                if (_poolLock.CurrentCount == 0)
                    _poolLock.Release();
            }
        }

        // 标记为忙碌
        string connectionId = Guid.NewGuid().ToString();
        connection.Id = connectionId;
        connection.LastUsedTime = DateTime.UtcNow;
        connection.SetPool(this); // 设置连接池引用

        _busyConnections[connectionId] = connection;
        return connection;
    }

    /// <summary>
    /// 释放连接回池
    /// </summary>
    public void ReleaseConnection(PooledConnection connection)
    {
        if (connection == null) return;

        try
        {
            if (_busyConnections.TryRemove(connection.Id, out _))
            {
                connection.LastUsedTime = DateTime.UtcNow;

                // 检查连接状态
                if (IsConnectionValid(connection))
                {
                    _availableConnections.Add(connection);
                }
                else
                {
                    // 连接无效，关闭并减少计数
                    SafeCloseConnection(connection);
                    Interlocked.Decrement(ref _totalConnections);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"释放连接失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 创建新连接
    /// </summary>
    private PooledConnection CreateNewConnection()
    {
        var connection = new DuckDBConnection(_options.ConnectionString);
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"PRAGMA threads={_options.ThreadsPerConnection};";
            command.ExecuteNonQuery();

            if (!string.IsNullOrEmpty(_options.MemoryLimit))
            {
                command.CommandText = $"PRAGMA memory_limit='{_options.MemoryLimit}';";
                command.ExecuteNonQuery();
            }

            if (_options.EnableCompression)
            {
                command.CommandText = $"PRAGMA force_compression='{_options.CompressionType}';";
                command.ExecuteNonQuery();
            }
        }

        var pooledConnection = new PooledConnection
        {
            Connection = connection,
            CreationTime = DateTime.UtcNow,
            LastUsedTime = DateTime.UtcNow
        };
        pooledConnection.SetPool(this); // 设置连接池引用
        return pooledConnection;
    }

    /// <summary>
    /// 检查连接有效性
    /// </summary>
    private bool IsConnectionValid(PooledConnection pooledConnection)
    {
        if (pooledConnection == null || pooledConnection.Connection == null)
            return false;

        try
        {
            var connection = pooledConnection.Connection;

            // 检查连接状态
            if (connection.State != ConnectionState.Open)
                return false;

            // 验证连接有效期
            if ((DateTime.UtcNow - pooledConnection.CreationTime).TotalHours >= _options.MaxConnectionLifetimeHours)
                return false;

            // 执行简单查询测试连接
            if (_options.TestConnectionOnBorrow)
            {
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.CommandTimeout = 5; // 短超时
                command.ExecuteScalar();
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.Debug($"连接验证失败: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// 安全关闭连接
    /// </summary>
    private void SafeCloseConnection(PooledConnection pooledConnection)
    {
        try
        {
            if (pooledConnection?.Connection != null)
            {
                if (pooledConnection.Connection.State != ConnectionState.Closed)
                    pooledConnection.Connection.Close();

                pooledConnection.Connection.Dispose();
                pooledConnection.Connection = null;
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"关闭连接失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 清理未使用连接（定期调用）
    /// </summary>
    public void CleanIdleConnections()
    {
        if (_disposed) return;

        try
        {
            // 只在连接数超过最小值时清理
            if (_totalConnections <= _options.MinConnections)
                return;

            int removedCount = 0;
            var now = DateTime.UtcNow;
            var idleThreshold = TimeSpan.FromSeconds(_options.MaxIdleTimeSeconds);

            var connectionsToCheck = _availableConnections.ToArray();
            _availableConnections.Clear();

            foreach (var conn in connectionsToCheck)
            {
                // 检查空闲时间
                if ((now - conn.LastUsedTime) > idleThreshold && _totalConnections > _options.MinConnections)
                {
                    SafeCloseConnection(conn);
                    Interlocked.Decrement(ref _totalConnections);
                    removedCount++;
                }
                else
                {
                    _availableConnections.Add(conn);
                }
            }

            if (removedCount > 0)
            {
                _logger.Debug($"已清理 {removedCount} 个空闲连接，当前总连接数: {_totalConnections}");
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"清理空闲连接失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 获取连接池状态
    /// </summary>
    public PoolStatus GetStatus()
    {
        return new PoolStatus
        {
            TotalConnections = _totalConnections,
            AvailableConnections = _availableConnections.Count,
            BusyConnections = _busyConnections.Count,
            MaxConnections = _options.MaxConnections,
            MinConnections = _options.MinConnections
        };
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;

        try
        {
            foreach (var conn in _availableConnections)
            {
                SafeCloseConnection(conn);
            }

            _availableConnections.Clear();

            foreach (var conn in _busyConnections.Values)
            {
                SafeCloseConnection(conn);
            }

            _busyConnections.Clear();

            _poolLock.Dispose();
        }
        catch (Exception ex)
        {
            _logger.Error($"释放连接池资源失败: {ex.Message}", ex);
        }

        _disposed = true;
    }
}

/// <summary>
/// 池化连接
/// </summary>
public class PooledConnection
{
    public string Id { get; set; }
    public DuckDBConnection Connection { get; set; }
    public DateTime CreationTime { get; set; }
    public DateTime LastUsedTime { get; set; }

    // 连接池引用，用于归还连接
    private DuckDBConnectionPool _pool;

    // 设置连接池引用
    internal void SetPool(DuckDBConnectionPool pool)
    {
        _pool = pool;
    }

    /// <summary>
    /// 将连接释放回池中
    /// </summary>
    public void Release()
    {
        _pool?.ReleaseConnection(this);
    }
}

/// <summary>
/// 连接池状态
/// </summary>
public class PoolStatus
{
    public int TotalConnections { get; set; }
    public int AvailableConnections { get; set; }
    public int BusyConnections { get; set; }
    public int MaxConnections { get; set; }
    public int MinConnections { get; set; }

    public override string ToString()
    {
        return $"总连接: {TotalConnections}, 可用: {AvailableConnections}, " +
               $"使用中: {BusyConnections}, 最大/最小: {MaxConnections}/{MinConnections}";
    }
}

/// <summary>
/// DuckDB连接池选项
/// </summary>
public class DuckDBPoolOptions
{
    public string ConnectionString { get; set; } = "Data Source=:memory:";
    public int MinConnections { get; set; } = 1;
    public int MaxConnections { get; set; } = 10;
    public int ThreadsPerConnection { get; set; } = 4;
    public string MemoryLimit { get; set; } = "2GB";
    public bool EnableCompression { get; set; } = true;
    public string CompressionType { get; set; } = "zstd";
    public int MaxIdleTimeSeconds { get; set; } = 300; // 空闲连接超时
    public double MaxConnectionLifetimeHours { get; set; } = 3; // 连接最大生命周期
    public TimeSpan ConnectionWaitTime { get; set; } = TimeSpan.FromSeconds(3); // 等待连接的时间
    public bool TestConnectionOnBorrow { get; set; } = false; // 借用时测试连接
}
