using System.Data;
using System.Diagnostics;
using DuckDB.NET.Data;

namespace Abp.DuckDB.FileQuery;

public static class QueryMetricsExtensions
{
    /// <summary>
    /// 执行DuckDB命令并记录指标日志
    /// </summary>
    public static async Task<T> ExecuteWithMetricsAsync<T>(
        this DuckDBCommand command,
        Func<DuckDBCommand, Task<T>> executeFunc,
        Action<string> logger)
    {
        var sw = Stopwatch.StartNew();
        string sql = command.CommandText;
            
        try
        {
            T result = await executeFunc(command);
            sw.Stop();
                
            // 记录基本指标
            logger($"[DuckDB指标] 执行时间: {sw.ElapsedMilliseconds}ms | SQL: {sql}");
                
            // 如果是IDataReader，记录行数（仅测试用）
            if (result is IDataReader reader)
            {
                int rowCount = 0;
                while (reader.Read()) rowCount++;
                logger($"[DuckDB指标] 结果行数: {rowCount}");
                reader.Close();
            }
                
            return result;
        }
        catch (Exception ex)
        {
            sw.Stop();
            logger($"[DuckDB指标] 执行失败 | 时间: {sw.ElapsedMilliseconds}ms | SQL: {sql} | 错误: {ex.Message}");
            throw;
        }
    }
        
    /// <summary>
    /// 使用同步方式执行DuckDB命令并记录指标日志
    /// </summary>
    public static T ExecuteWithMetrics<T>(
        this DuckDBCommand command,
        Func<DuckDBCommand, T> executeFunc,
        Action<string> logger)
    {
        var sw = Stopwatch.StartNew();
        string sql = command.CommandText;
            
        try
        {
            T result = executeFunc(command);
            sw.Stop();
                
            logger($"[DuckDB指标] 执行时间: {sw.ElapsedMilliseconds}ms | SQL: {sql}");
            return result;
        }
        catch (Exception ex)
        {
            sw.Stop();
            logger($"[DuckDB指标] 执行失败 | 时间: {sw.ElapsedMilliseconds}ms | SQL: {sql} | 错误: {ex.Message}");
            throw;
        }
    }
}
