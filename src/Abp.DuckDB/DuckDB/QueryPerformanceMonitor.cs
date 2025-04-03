using System.Collections.Concurrent;

namespace Abp.DuckDB;

/// <summary>
/// DuckDB查询性能监控系统，用于收集和分析查询性能指标
/// </summary>
public class QueryPerformanceMonitor
{
    private readonly ConcurrentDictionary<string, QueryPerformanceMetrics> _metricsStore = new();
    private readonly ConcurrentQueue<QueryExecutionLog> _recentExecutions = new();
    private readonly int _maxRecentExecutions = 1000;

    /// <summary>
    /// 记录查询执行性能指标
    /// </summary>
    public void RecordQueryExecution(
        string queryType,
        string sql,
        int fileCount,
        int resultCount,
        long executionTimeMs,
        bool isSuccess,
        Exception exception = null)
    {
        // 更新累积指标
        _metricsStore.AddOrUpdate(
            queryType,
            _ => new QueryPerformanceMetrics
            {
                TotalExecutions = 1,
                SuccessfulExecutions = isSuccess ? 1 : 0,
                FailedExecutions = isSuccess ? 0 : 1,
                TotalDurationMs = executionTimeMs,
                MinDurationMs = executionTimeMs,
                MaxDurationMs = executionTimeMs,
                TotalFilesProcessed = fileCount,
                TotalRecordsProcessed = resultCount,
                LastExecutionTime = DateTime.UtcNow
            },
            (_, metrics) =>
            {
                Interlocked.Increment(ref metrics.TotalExecutions);
                if (isSuccess)
                    Interlocked.Increment(ref metrics.SuccessfulExecutions);
                else
                    Interlocked.Increment(ref metrics.FailedExecutions);

                Interlocked.Add(ref metrics.TotalDurationMs, executionTimeMs);

                // 原子更新最小值
                long currentMin;
                do
                {
                    currentMin = metrics.MinDurationMs;
                    if (executionTimeMs >= currentMin) break;
                } while (Interlocked.CompareExchange(ref metrics.MinDurationMs, executionTimeMs, currentMin) != currentMin);

                // 原子更新最大值
                long currentMax;
                do
                {
                    currentMax = metrics.MaxDurationMs;
                    if (executionTimeMs <= currentMax) break;
                } while (Interlocked.CompareExchange(ref metrics.MaxDurationMs, executionTimeMs, currentMax) != currentMax);

                Interlocked.Add(ref metrics.TotalFilesProcessed, fileCount);
                Interlocked.Add(ref metrics.TotalRecordsProcessed, resultCount);
                metrics.LastExecutionTime = DateTime.UtcNow;
                return metrics;
            });

        // 记录详细日志
        var logEntry = new QueryExecutionLog
        {
            QueryType = queryType,
            Sql = sql,
            FileCount = fileCount,
            RecordCount = resultCount,
            ExecutionTimeMs = executionTimeMs,
            IsSuccess = isSuccess,
            ExecutionTime = DateTime.UtcNow,
            Exception = exception?.Message
        };

        _recentExecutions.Enqueue(logEntry);

        // 控制队列大小
        while (_recentExecutions.Count > _maxRecentExecutions)
        {
            _recentExecutions.TryDequeue(out _);
        }
    }

    /// <summary>
    /// 获取指定查询类型的性能指标
    /// </summary>
    public QueryPerformanceMetrics GetMetricsForQueryType(string queryType)
    {
        _metricsStore.TryGetValue(queryType, out var metrics);
        return metrics;
    }

    /// <summary>
    /// 获取所有查询类型的性能指标
    /// </summary>
    public Dictionary<string, QueryPerformanceMetrics> GetAllMetrics()
    {
        return _metricsStore.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
    }

    /// <summary>
    /// 获取最近的查询执行日志
    /// </summary>
    public List<QueryExecutionLog> GetRecentExecutions(int count = 100)
    {
        return _recentExecutions.Take(Math.Min(count, _recentExecutions.Count)).ToList();
    }

    /// <summary>
    /// 重置性能指标
    /// </summary>
    public void ResetMetrics(string queryType = null)
    {
        if (string.IsNullOrEmpty(queryType))
        {
            _metricsStore.Clear();
        }
        else
        {
            _metricsStore.TryRemove(queryType, out _);
        }
    }

    /// <summary>
    /// 清除最近的执行日志
    /// </summary>
    public void ClearExecutionLogs()
    {
        while (_recentExecutions.TryDequeue(out _))
        {
        }
    }

    /// <summary>
    /// 生成性能报告
    /// </summary>
    public QueryPerformanceReport GenerateReport()
    {
        var report = new QueryPerformanceReport
        {
            GenerationTime = DateTime.UtcNow,
            TotalQueries = _metricsStore.Values.Sum(m => m.TotalExecutions),
            SuccessfulQueries = _metricsStore.Values.Sum(m => m.SuccessfulExecutions),
            FailedQueries = _metricsStore.Values.Sum(m => m.FailedExecutions),
            TotalDurationMs = _metricsStore.Values.Sum(m => m.TotalDurationMs),
            QueryTypeMetrics = _metricsStore.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
        };

        return report;
    }
}

/// <summary>
/// 查询性能指标类
/// </summary>
public class QueryPerformanceMetrics
{
    public long TotalExecutions;
    public long SuccessfulExecutions;
    public long FailedExecutions;
    public long TotalDurationMs;
    public long MinDurationMs = long.MaxValue;
    public long MaxDurationMs;
    public int TotalFilesProcessed;
    public int TotalRecordsProcessed;
    public DateTime LastExecutionTime;

    // 计算属性
    public double AverageDurationMs => TotalExecutions > 0 ? (double)TotalDurationMs / TotalExecutions : 0;
    public double SuccessRate => TotalExecutions > 0 ? (double)SuccessfulExecutions / TotalExecutions * 100 : 0;
    public double AverageRecordsPerFile => TotalFilesProcessed > 0 ? (double)TotalRecordsProcessed / TotalFilesProcessed : 0;
}

/// <summary>
/// 查询执行日志条目
/// </summary>
public class QueryExecutionLog
{
    public string QueryType { get; set; }
    public string Sql { get; set; }
    public int FileCount { get; set; }
    public int RecordCount { get; set; }
    public long ExecutionTimeMs { get; set; }
    public bool IsSuccess { get; set; }
    public DateTime ExecutionTime { get; set; }
    public string Exception { get; set; }
}

/// <summary>
/// 性能报告
/// </summary>
public class QueryPerformanceReport
{
    public DateTime GenerationTime { get; set; }
    public long TotalQueries { get; set; }
    public long SuccessfulQueries { get; set; }
    public long FailedQueries { get; set; }
    public long TotalDurationMs { get; set; }
    public Dictionary<string, QueryPerformanceMetrics> QueryTypeMetrics { get; set; }

    // 计算属性
    public double OverallSuccessRate => TotalQueries > 0 ? (double)SuccessfulQueries / TotalQueries * 100 : 0;
    public double AverageQueryTimeMs => TotalQueries > 0 ? (double)TotalDurationMs / TotalQueries : 0;
}
