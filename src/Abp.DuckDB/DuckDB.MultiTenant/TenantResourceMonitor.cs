using System.Collections.Concurrent;
using System.Diagnostics;
using Castle.Core.Logging;

namespace Abp.DuckDB.MultiTenant;

/// <summary>
/// 租户资源监控器，负责跟踪和限制租户资源使用
/// </summary>
public class TenantResourceMonitor : IDisposable
{
    private readonly ILogger _logger;
    private readonly Timer _statsCleanupTimer;
    private readonly ConcurrentDictionary<string, TenantResourceStats> _tenantStats = new();
    private readonly ConcurrentDictionary<string, TenantResourceConfig> _tenantConfigs = new();
    private bool _disposed;

    /// <summary>
    /// 默认查询超时（毫秒）
    /// </summary>
    public int DefaultQueryTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// 默认每秒最大查询数
    /// </summary>
    public int DefaultMaxQueriesPerSecond { get; set; } = 50;

    /// <summary>
    /// 默认单次查询最大结果集大小
    /// </summary>
    public int DefaultMaxResultSize { get; set; } = 10000;

    /// <summary>
    /// 统计保留时间（小时）
    /// </summary>
    public int StatsRetentionHours { get; set; } = 24;

    /// <summary>
    /// 创建租户资源监控器实例
    /// </summary>
    /// <param name="logger">日志记录器</param>
    public TenantResourceMonitor(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _statsCleanupTimer = new Timer(CleanupStatsCallback, null, 
            TimeSpan.FromHours(1), TimeSpan.FromHours(1));
    }

    /// <summary>
    /// 设置租户资源配置
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <param name="config">资源配置</param>
    public void SetTenantConfig(string tenantId, TenantResourceConfig config)
    {
        if (string.IsNullOrEmpty(tenantId))
            throw new ArgumentNullException(nameof(tenantId));
                
        if (config == null)
            throw new ArgumentNullException(nameof(config));
                
        _tenantConfigs[tenantId] = config;
        _logger.Info($"已设置租户 {tenantId} 的资源配置: 超时={config.QueryTimeoutMs}ms, " +
                     $"最大QPS={config.MaxQueriesPerSecond}, 最大结果集={config.MaxResultSize}");
    }

    /// <summary>
    /// 获取租户资源配置
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>资源配置</returns>
    public TenantResourceConfig GetTenantConfig(string tenantId)
    {
        if (string.IsNullOrEmpty(tenantId))
            throw new ArgumentNullException(nameof(tenantId));
                
        return _tenantConfigs.GetOrAdd(tenantId, _ => new TenantResourceConfig
        {
            QueryTimeoutMs = DefaultQueryTimeoutMs,
            MaxQueriesPerSecond = DefaultMaxQueriesPerSecond,
            MaxResultSize = DefaultMaxResultSize
        });
    }

    /// <summary>
    /// 记录查询执行情况
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <param name="duration">查询执行时长</param>
    /// <param name="rowsAffected">影响的行数</param>
    /// <param name="isSuccess">是否成功</param>
    /// <param name="errorMessage">错误信息</param>
    public void TrackQueryExecution(string tenantId, TimeSpan duration, int rowsAffected = 0, 
        bool isSuccess = true, string errorMessage = null)
    {
        if (string.IsNullOrEmpty(tenantId))
            return;

        var stats = _tenantStats.GetOrAdd(tenantId, _ => new TenantResourceStats(tenantId));
        stats.TrackQuery(duration, rowsAffected, isSuccess, errorMessage);

        // 记录长查询
        if (duration.TotalMilliseconds > 1000)
        {
            _logger.Debug($"租户 {tenantId} 的长查询：{duration.TotalMilliseconds:F1}ms, 影响行数：{rowsAffected}");
        }
    }

    /// <summary>
    /// 获取租户当前资源使用情况
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>资源使用统计</returns>
    public TenantResourceUsage GetResourceUsage(string tenantId)
    {
        if (string.IsNullOrEmpty(tenantId))
            throw new ArgumentNullException(nameof(tenantId));
                
        if (_tenantStats.TryGetValue(tenantId, out var stats))
        {
            var usage = stats.GetCurrentUsage();
            var config = GetTenantConfig(tenantId);
                
            return new TenantResourceUsage
            {
                TenantId = tenantId,
                QueriesPerSecond = usage.QueriesPerSecond,
                AverageQueryTimeMs = usage.AverageQueryTimeMs,
                TotalQueries = usage.TotalQueries,
                FailedQueries = usage.FailedQueries,
                MaxQueryTimeMs = usage.MaxQueryTimeMs,
                MaxRowsAffected = usage.MaxRowsAffected,
                QueryTimeoutMs = config.QueryTimeoutMs,
                MaxAllowedQps = config.MaxQueriesPerSecond,
                IsThrottled = usage.QueriesPerSecond > config.MaxQueriesPerSecond
            };
        }
            
        return new TenantResourceUsage 
        { 
            TenantId = tenantId,
            QueryTimeoutMs = GetTenantConfig(tenantId).QueryTimeoutMs 
        };
    }

    /// <summary>
    /// 检查租户是否超过配额
    /// </summary>
    /// <param name="tenantId">租户ID</param>
    /// <returns>如果超过配额返回true</returns>
    public bool IsQuotaExceeded(string tenantId)
    {
        if (string.IsNullOrEmpty(tenantId))
            return false;
                
        if (_tenantStats.TryGetValue(tenantId, out var stats))
        {
            var config = GetTenantConfig(tenantId);
            var qps = stats.GetCurrentQueriesPerSecond();
                
            return qps > config.MaxQueriesPerSecond;
        }
            
        return false;
    }

    /// <summary>
    /// 创建具有监控功能的查询执行包装器
    /// </summary>
    /// <typeparam name="T">查询结果类型</typeparam>
    /// <param name="tenantId">租户ID</param>
    /// <param name="queryFunc">查询函数</param>
    /// <returns>查询执行包装器</returns>
    public async Task<T> ExecuteWithMonitoringAsync<T>(string tenantId, Func<Task<T>> queryFunc)
    {
        if (string.IsNullOrEmpty(tenantId))
            throw new ArgumentNullException(nameof(tenantId));
                
        if (queryFunc == null)
            throw new ArgumentNullException(nameof(queryFunc));
                
        // 检查配额
        if (IsQuotaExceeded(tenantId))
        {
            _logger.Warn($"租户 {tenantId} 超过查询配额限制，请求已被限流");
            throw new TenantQuotaExceededException($"租户 {tenantId} 超过查询配额限制，请稍后重试");
        }

        var config = GetTenantConfig(tenantId);
        var stopwatch = Stopwatch.StartNew();
            
        try
        {
            // 使用超时包装查询
            var queryTask = queryFunc();
            var timeoutTask = Task.Delay(config.QueryTimeoutMs);
                
            var completedTask = await Task.WhenAny(queryTask, timeoutTask);
                
            if (completedTask == timeoutTask)
            {
                stopwatch.Stop();
                TrackQueryExecution(tenantId, stopwatch.Elapsed, 0, false, "查询超时");
                _logger.Error($"租户 {tenantId} 的查询执行超时，已超过 {config.QueryTimeoutMs}ms");
                throw new TimeoutException($"租户 {tenantId} 的查询执行超时，已超过 {config.QueryTimeoutMs}ms");
            }

            var result = await queryTask;
            stopwatch.Stop();
                
            // 如果结果是集合，检查大小限制
            if (result is ICollection<object> collection && collection.Count > config.MaxResultSize)
            {
                TrackQueryExecution(tenantId, stopwatch.Elapsed, collection.Count, false, "结果集过大");
                _logger.Error($"租户 {tenantId} 的查询结果集过大: {collection.Count} (最大限制: {config.MaxResultSize})");
                throw new TenantResourceLimitException($"查询结果集超过限制: {collection.Count} (最大限制: {config.MaxResultSize})");
            }
                
            // 记录成功查询
            TrackQueryExecution(tenantId, stopwatch.Elapsed, result is ICollection<object> ? ((ICollection<object>)result).Count : 0);
                
            return result;
        }
        catch (Exception ex) when (!(ex is TimeoutException || ex is TenantQuotaExceededException || ex is TenantResourceLimitException))
        {
            // 记录失败查询
            stopwatch.Stop();
            TrackQueryExecution(tenantId, stopwatch.Elapsed, 0, false, ex.Message);
            _logger.Error($"租户 {tenantId} 的查询执行异常: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 获取所有租户的资源使用摘要
    /// </summary>
    /// <returns>资源使用摘要集合</returns>
    public IEnumerable<TenantResourceUsage> GetAllTenantsResourceUsage()
    {
        return _tenantStats.Values
            .Select(stats => 
            {
                var usage = stats.GetCurrentUsage();
                var config = GetTenantConfig(stats.TenantId);
                    
                return new TenantResourceUsage
                {
                    TenantId = stats.TenantId,
                    QueriesPerSecond = usage.QueriesPerSecond,
                    AverageQueryTimeMs = usage.AverageQueryTimeMs,
                    TotalQueries = usage.TotalQueries,
                    FailedQueries = usage.FailedQueries,
                    MaxQueryTimeMs = usage.MaxQueryTimeMs,
                    MaxRowsAffected = usage.MaxRowsAffected,
                    QueryTimeoutMs = config.QueryTimeoutMs,
                    MaxAllowedQps = config.MaxQueriesPerSecond,
                    IsThrottled = usage.QueriesPerSecond > config.MaxQueriesPerSecond
                };
            })
            .ToList();
    }

    /// <summary>
    /// 清理过期统计信息
    /// </summary>
    private void CleanupStatsCallback(object state)
    {
        try
        {
            var cutoffTime = DateTime.UtcNow.AddHours(-StatsRetentionHours);
            int cleanedCount = 0;
                
            foreach (var stats in _tenantStats.Values)
            {
                int removed = stats.CleanupHistory(cutoffTime);
                cleanedCount += removed;
            }
                
            if (cleanedCount > 0)
            {
                _logger.Debug($"清理了 {cleanedCount} 条过期的查询统计记录");
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"清理统计记录时出错: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _statsCleanupTimer?.Dispose();
        }
    }
}

/// <summary>
/// 租户资源统计
/// </summary>
internal class TenantResourceStats
{
    public string TenantId { get; }
        
    private readonly ConcurrentQueue<QueryExecutionRecord> _recentQueries = new();
    private readonly ConcurrentQueue<QueryExecutionRecord> _errorQueries = new();
    private int _totalQueries;
    private int _failedQueries;
    private long _totalQueryTimeMs;
    private long _maxQueryTimeMs;
    private int _maxRowsAffected;
    private readonly object _statsLock = new object();

    public TenantResourceStats(string tenantId)
    {
        TenantId = tenantId;
    }

    /// <summary>
    /// 记录查询执行
    /// </summary>
    public void TrackQuery(TimeSpan duration, int rowsAffected, bool isSuccess, string errorMessage)
    {
        var record = new QueryExecutionRecord
        {
            Timestamp = DateTime.UtcNow,
            DurationMs = (long)duration.TotalMilliseconds,
            RowsAffected = rowsAffected,
            IsSuccess = isSuccess,
            ErrorMessage = errorMessage
        };
            
        _recentQueries.Enqueue(record);
            
        // 限制队列大小
        while (_recentQueries.Count > 1000)
        {
            _recentQueries.TryDequeue(out _);
        }
            
        if (!isSuccess)
        {
            _errorQueries.Enqueue(record);
                
            // 限制错误队列大小
            while (_errorQueries.Count > 100)
            {
                _errorQueries.TryDequeue(out _);
            }
        }
            
        // 更新汇总统计
        lock (_statsLock)
        {
            _totalQueries++;
                
            if (!isSuccess)
            {
                _failedQueries++;
            }
                
            _totalQueryTimeMs += (long)duration.TotalMilliseconds;
                
            if ((long)duration.TotalMilliseconds > _maxQueryTimeMs)
            {
                _maxQueryTimeMs = (long)duration.TotalMilliseconds;
            }
                
            if (rowsAffected > _maxRowsAffected)
            {
                _maxRowsAffected = rowsAffected;
            }
        }
    }

    /// <summary>
    /// 清理历史数据
    /// </summary>
    public int CleanupHistory(DateTime cutoffTime)
    {
        int removedCount = 0;
            
        // 清理最近查询
        while (_recentQueries.TryPeek(out var oldestQuery) && oldestQuery.Timestamp < cutoffTime)
        {
            if (_recentQueries.TryDequeue(out _))
            {
                removedCount++;
            }
        }
            
        // 清理错误查询
        while (_errorQueries.TryPeek(out var oldestError) && oldestError.Timestamp < cutoffTime)
        {
            _errorQueries.TryDequeue(out _);
        }
            
        return removedCount;
    }

    /// <summary>
    /// 获取当前每秒查询数
    /// </summary>
    public double GetCurrentQueriesPerSecond()
    {
        var now = DateTime.UtcNow;
        var oneSecondAgo = now.AddSeconds(-1);
            
        int count = _recentQueries.Count(q => q.Timestamp >= oneSecondAgo);
        return count;
    }

    /// <summary>
    /// 获取当前使用情况
    /// </summary>
    public TenantResourceUsage GetCurrentUsage()
    {
        var now = DateTime.UtcNow;
        var oneSecondAgo = now.AddSeconds(-1);
        var oneMinuteAgo = now.AddMinutes(-1);
            
        // 计算实时QPS
        int queriesLastSecond = _recentQueries.Count(q => q.Timestamp >= oneSecondAgo);
            
        // 计算最近一分钟的平均查询时间
        var recentQueries = _recentQueries.Where(q => q.Timestamp >= oneMinuteAgo).ToList();
        double avgQueryTime = recentQueries.Any() 
            ? recentQueries.Average(q => q.DurationMs) 
            : 0;
            
        lock (_statsLock)
        {
            return new TenantResourceUsage
            {
                TenantId = TenantId,
                QueriesPerSecond = queriesLastSecond,
                AverageQueryTimeMs = avgQueryTime,
                TotalQueries = _totalQueries,
                FailedQueries = _failedQueries,
                MaxQueryTimeMs = _maxQueryTimeMs,
                MaxRowsAffected = _maxRowsAffected,
                RecentErrors = _errorQueries.Take(10).ToList()
            };
        }
    }
}

/// <summary>
/// 查询执行记录
/// </summary>
public class QueryExecutionRecord
{
    /// <summary>
    /// 执行时间戳
    /// </summary>
    public DateTime Timestamp { get; set; }
        
    /// <summary>
    /// 执行时长（毫秒）
    /// </summary>
    public long DurationMs { get; set; }
        
    /// <summary>
    /// 影响的行数
    /// </summary>
    public int RowsAffected { get; set; }
        
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool IsSuccess { get; set; }
        
    /// <summary>
    /// 错误消息
    /// </summary>
    public string ErrorMessage { get; set; }
}

/// <summary>
/// 租户资源配置
/// </summary>
public class TenantResourceConfig
{
    /// <summary>
    /// 查询超时（毫秒）
    /// </summary>
    public int QueryTimeoutMs { get; set; }
        
    /// <summary>
    /// 最大每秒查询数
    /// </summary>
    public int MaxQueriesPerSecond { get; set; }
        
    /// <summary>
    /// 最大结果集大小
    /// </summary>
    public int MaxResultSize { get; set; }
}

/// <summary>
/// 租户资源使用情况
/// </summary>
public class TenantResourceUsage
{
    /// <summary>
    /// 租户ID
    /// </summary>
    public string TenantId { get; set; }
        
    /// <summary>
    /// 当前每秒查询数
    /// </summary>
    public double QueriesPerSecond { get; set; }
        
    /// <summary>
    /// 平均查询时间（毫秒）
    /// </summary>
    public double AverageQueryTimeMs { get; set; }
        
    /// <summary>
    /// 总查询次数
    /// </summary>
    public int TotalQueries { get; set; }
        
    /// <summary>
    /// 失败查询次数
    /// </summary>
    public int FailedQueries { get; set; }
        
    /// <summary>
    /// 最长查询时间（毫秒）
    /// </summary>
    public long MaxQueryTimeMs { get; set; }
        
    /// <summary>
    /// 最大影响行数
    /// </summary>
    public int MaxRowsAffected { get; set; }
        
    /// <summary>
    /// 查询超时设置（毫秒）
    /// </summary>
    public int QueryTimeoutMs { get; set; }
        
    /// <summary>
    /// 最大允许QPS
    /// </summary>
    public int MaxAllowedQps { get; set; }
        
    /// <summary>
    /// 是否被限流
    /// </summary>
    public bool IsThrottled { get; set; }
        
    /// <summary>
    /// 最近错误
    /// </summary>
    public List<QueryExecutionRecord> RecentErrors { get; set; } = new List<QueryExecutionRecord>();
        
    /// <summary>
    /// 成功率
    /// </summary>
    public double SuccessRate => TotalQueries > 0 ? 100.0 * (TotalQueries - FailedQueries) / TotalQueries : 100.0;
}

/// <summary>
/// 租户配额超出异常
/// </summary>
public class TenantQuotaExceededException : Exception
{
    public TenantQuotaExceededException(string message) : base(message) { }
}

/// <summary>
/// 租户资源限制异常
/// </summary>
public class TenantResourceLimitException : Exception
{
    public TenantResourceLimitException(string message) : base(message) { }
}
