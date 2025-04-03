namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB性能监控接口，负责查询性能的监控、统计和分析
/// </summary>
public interface IDuckDBPerformanceMonitor
{
    /// <summary>
    /// 分析查询计划
    /// </summary>
    Task<string> AnalyzeQueryPlanAsync(string sql);

    /// <summary>
    /// 获取性能统计报告
    /// </summary>
    QueryPerformanceReport GetPerformanceReport();

    /// <summary>
    /// 获取查询类型的性能指标
    /// </summary>
    QueryPerformanceMetrics GetMetricsForQueryType(string queryType);

    /// <summary>
    /// 重置性能指标
    /// </summary>
    void ResetPerformanceMetrics();

    /// <summary>
    /// 获取最近的查询执行日志
    /// </summary>
    List<QueryExecutionLog> GetRecentExecutions(int count = 100);

    /// <summary>
    /// 清除最近的执行日志
    /// </summary>
    void ClearExecutionLogs();
}
