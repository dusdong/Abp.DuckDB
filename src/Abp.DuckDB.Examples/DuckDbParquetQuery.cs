using System.Data;
using System.Data.Common;
using System.Diagnostics;
using DuckDB.NET.Data;

namespace Abp.DuckDB.Examples;

/// <summary>
/// DuckDB Parquet查询服务，提供对Parquet文件的SQL查询功能
/// </summary>
public class DuckDbParquetQueryService : IDisposable
{
    private readonly DuckDBConnection _connection;
    private bool _disposed = false;
    private readonly int _defaultTimeout;

    /// <summary>
    /// 初始化DuckDB Parquet查询服务
    /// </summary>
    /// <param name="connectionString">连接字符串，默认为内存数据库</param>
    /// <param name="timeoutInSeconds">SQL命令超时时间(秒)</param>
    public DuckDbParquetQueryService(string connectionString = "Data Source=:memory:", int timeoutInSeconds = 300)
    {
        _connection = new DuckDBConnection(connectionString);
        _connection.Open();
        _defaultTimeout = timeoutInSeconds;
    }

    /// <summary>
    /// 执行SQL查询并以表格形式返回结果
    /// </summary>
    /// <param name="sql">SQL查询语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>DataTable形式的结果集</returns>
    public async Task<DataTable> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(DuckDbParquetQueryService));

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = _defaultTimeout;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var dataTable = new DataTable();
        dataTable.Load(reader);
        return dataTable;
    }

    /// <summary>
    /// 执行SQL查询并返回动态对象列表
    /// </summary>
    /// <param name="sql">SQL查询语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>动态对象列表</returns>
    public async Task<List<dynamic>> ExecuteQueryDynamicAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(DuckDbParquetQueryService));

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = _defaultTimeout;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await MapToDynamicListAsync(reader, cancellationToken);
    }

    /// <summary>
    /// 执行SQL查询并返回强类型对象列表
    /// </summary>
    /// <typeparam name="T">返回对象类型</typeparam>
    /// <param name="sql">SQL查询语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>强类型对象列表</returns>
    public async Task<List<T>> ExecuteQueryAsync<T>(string sql, CancellationToken cancellationToken = default) where T : new()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(DuckDbParquetQueryService));

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = _defaultTimeout;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await MapToListAsync<T>(reader, cancellationToken);
    }

    /// <summary>
    /// 执行SQL查询并通过委托处理结果
    /// </summary>
    /// <param name="sql">SQL查询语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>处理的行数</returns>
    public async Task<QueryStats> ExecuteQueryWithHandlerAsync(string sql, 
        Func<DbDataReader, CancellationToken, Task> resultHandler, 
        CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(DuckDbParquetQueryService));

        var stopwatch = Stopwatch.StartNew();
            
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = _defaultTimeout;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
            
        // 执行用户提供的处理程序
        await resultHandler(reader, cancellationToken);
            
        stopwatch.Stop();
            
        return new QueryStats 
        {
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };
    }
        
    /// <summary>
    /// 执行SQL查询并获取单个值
    /// </summary>
    /// <typeparam name="T">返回值类型</typeparam>
    /// <param name="sql">SQL查询语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>查询结果的第一行第一列</returns>
    public async Task<T> ExecuteScalarAsync<T>(string sql, CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(DuckDbParquetQueryService));

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = _defaultTimeout;

        var result = await command.ExecuteScalarAsync(cancellationToken);
            
        if (result == null || result == DBNull.Value)
            return default;
                
        return (T)Convert.ChangeType(result, typeof(T));
    }
        
    /// <summary>
    /// 打印查询结果到控制台
    /// </summary>
    /// <param name="sql">SQL查询语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>查询统计信息</returns>
    public async Task<QueryStats> PrintQueryResultsAsync(string sql, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
            
        try
        {
            System.Console.WriteLine("SQL查询: ");
            System.Console.WriteLine("----------------------------------------------------");
            System.Console.WriteLine(sql);
            System.Console.WriteLine("----------------------------------------------------");

            using var command = _connection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = _defaultTimeout;

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
                
            // 打印结果表头
            System.Console.WriteLine("\n查询结果:");
            System.Console.WriteLine("----------------------------------------------------");
                
            // 获取列宽度以进行漂亮的格式化
            var columnWidths = new int[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columnWidths[i] = Math.Max(reader.GetName(i).Length, 10);
            }
                
            // 打印表头
            for (int i = 0; i < reader.FieldCount; i++)
            {
                System.Console.Write($"{reader.GetName(i).PadRight(columnWidths[i])} | ");
            }
            System.Console.WriteLine();
                
            // 打印分隔线
            for (int i = 0; i < reader.FieldCount; i++)
            {
                System.Console.Write(new string('-', columnWidths[i]) + " | ");
            }
            System.Console.WriteLine();
                
            // 打印数据行
            int rowCount = 0;
            decimal totalSum = 0;
            bool hasSumColumn = false;
            int sumColumnIndex = -1;
                
            // 检查是否有Sum列
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals("Sum", StringComparison.OrdinalIgnoreCase))
                {
                    hasSumColumn = true;
                    sumColumnIndex = i;
                    break;
                }
            }
                
            while (await reader.ReadAsync(cancellationToken))
            {
                rowCount++;
                    
                // 如果有Sum列，尝试累计总和
                if (hasSumColumn && !reader.IsDBNull(sumColumnIndex))
                {
                    var sum = reader.GetValue(sumColumnIndex);
                    if (sum is decimal decimalSum)
                    {
                        totalSum += decimalSum;
                    }
                    else if (sum != null && decimal.TryParse(sum.ToString(), out decimal parsedSum))
                    {
                        totalSum += parsedSum;
                    }
                }
                    
                // 打印行数据
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i).ToString();
                    System.Console.Write($"{value.PadRight(columnWidths[i])} | ");
                }
                System.Console.WriteLine();
            }
                
            // 打印汇总信息
            System.Console.WriteLine("----------------------------------------------------");
            System.Console.WriteLine($"总行数: {rowCount}");
            if (hasSumColumn)
            {
                System.Console.WriteLine($"Sum列总计: {totalSum:N0}");
            }
                
            stopwatch.Stop();
                
            return new QueryStats
            {
                RowCount = rowCount,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                TotalSum = hasSumColumn ? totalSum : null
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            System.Console.WriteLine($"查询执行失败: {ex.Message}");
            throw;
        }
    }
        
    #region 辅助方法
        
    private async Task<List<T>> MapToListAsync<T>(DbDataReader reader, CancellationToken cancellationToken) where T : new()
    {
        var results = new List<T>();
        var properties = typeof(T).GetProperties();
            
        // 创建列名到属性的映射
        var columnMap = new Dictionary<string, System.Reflection.PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in properties)
        {
            columnMap[property.Name] = property;
        }
            
        while (await reader.ReadAsync(cancellationToken))
        {
            var item = new T();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                if (columnMap.TryGetValue(columnName, out var property))
                {
                    if (!reader.IsDBNull(i))
                    {
                        try
                        {
                            var value = reader.GetValue(i);
                            if (property.PropertyType != value.GetType() && value is IConvertible)
                            {
                                value = Convert.ChangeType(value, property.PropertyType);
                            }
                            property.SetValue(item, value);
                        }
                        catch
                        {
                            // 忽略类型转换错误
                        }
                    }
                }
            }
            results.Add(item);
        }
            
        return results;
    }
        
    private async Task<List<dynamic>> MapToDynamicListAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        var results = new List<dynamic>();
            
        while (await reader.ReadAsync(cancellationToken))
        {
            var expandoObject = new System.Dynamic.ExpandoObject() as IDictionary<string, object>;
                
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                expandoObject[columnName] = value;
            }
                
            results.Add(expandoObject);
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
                _connection?.Close();
                _connection?.Dispose();
            }
                
            _disposed = true;
        }
    }
        
    ~DuckDbParquetQueryService()
    {
        Dispose(false);
    }
        
    #endregion
}
    
/// <summary>
/// 查询统计信息
/// </summary>
public class QueryStats
{
    /// <summary>
    /// 查询返回的行数
    /// </summary>
    public int RowCount { get; set; }
        
    /// <summary>
    /// 查询执行时间（毫秒）
    /// </summary>
    public long ExecutionTimeMs { get; set; }
        
    /// <summary>
    /// Sum列的总和（如果适用）
    /// </summary>
    public decimal? TotalSum { get; set; }
}
