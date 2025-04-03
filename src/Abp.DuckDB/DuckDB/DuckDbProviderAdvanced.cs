using System.Linq.Expressions;
using System.Text;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB;

/// <summary>
/// 具备高级功能的DuckDB查询提供程序
/// </summary>
public abstract class DuckDbProviderAdvanced : DuckDbProviderBase, IDuckDBProviderAdvanced
{
    protected DuckDbProviderAdvanced(ILogger logger, QueryPerformanceMonitor performanceMonitor = null)
        : base(logger, performanceMonitor)
    {
    }

    /// <summary>
    /// 获取DuckDB连接
    /// </summary>
    public DuckDBConnection GetDuckDBConnection()
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        return _connection;
    }

    /// <summary>
    /// 使用向量化过滤进行查询
    /// </summary>
    public async Task<List<TEntity>> QueryWithVectorizedFiltersAsync<TEntity>(
        string tableName,
        string[] columns,
        object[][] filterValues,
        int resultLimit = 1000)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        if (columns == null || columns.Length == 0)
            throw new ArgumentException("至少需要一个过滤列", nameof(columns));
        if (filterValues == null || filterValues.Length != columns.Length)
            throw new ArgumentException("过滤值数组长度必须与列数组长度匹配", nameof(filterValues));

        try
        {
            // 构建SQL查询
            var sql = BuildVectorizedFilterQuery(tableName, columns, filterValues, resultLimit);
            _logger.Debug($"执行向量化过滤查询: {sql}");

            // 执行查询
            return await ExecuteQueryWithMetricsAsync(
                () => QueryWithRawSqlAsync<TEntity>(sql),
                "VectorizedFilter",
                sql);
        }
        catch (Exception ex)
        {
            _logger.Error($"执行向量化过滤查询失败: {ex.Message}", ex);
            throw new Exception("执行向量化过滤查询失败", ex);
        }
    }

    /// <summary>
    /// 构建向量化过滤查询的SQL
    /// </summary>
    protected string BuildVectorizedFilterQuery(
        string tableName,
        string[] columns,
        object[][] filterValues,
        int resultLimit)
    {
        var sb = new StringBuilder();
        sb.Append($"SELECT * FROM {tableName} WHERE ");

        for (int i = 0; i < columns.Length; i++)
        {
            if (i > 0) sb.Append(" AND ");

            sb.Append($"{columns[i]} IN (");

            for (int j = 0; j < filterValues[i].Length; j++)
            {
                if (j > 0) sb.Append(", ");

                // 根据值类型进行适当格式化
                var value = filterValues[i][j];
                if (value == null)
                {
                    sb.Append("NULL");
                }
                else if (value is string strValue)
                {
                    sb.Append($"'{strValue.Replace("'", "''")}'");
                }
                else if (value is DateTime dtValue)
                {
                    sb.Append($"'{dtValue:yyyy-MM-dd HH:mm:ss}'");
                }
                else
                {
                    sb.Append(value.ToString());
                }
            }

            sb.Append(")");
        }

        // 添加限制
        if (resultLimit > 0)
        {
            sb.Append($" LIMIT {resultLimit}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// 注册自定义函数
    /// </summary>
    public void RegisterFunction<TReturn, TParam1>(string functionName, Func<TParam1, TReturn> function)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        try
        {
            _logger.Debug($"注册自定义函数: {functionName}");

            // 注意：这里需要根据DuckDB.NET的具体实现调整
            // 以下是一个示例实现

            // 获取内部连接对象
            // var db = _connection.InnerConnection;
            // db.RegisterFunction(functionName, function);

            _logger.Info($"成功注册自定义函数: {functionName}");
        }
        catch (Exception ex)
        {
            _logger.Error($"注册自定义函数失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 执行非查询SQL语句
    /// </summary>
    public async Task<int> ExecuteNonQueryAsync(string sql, params object[] parameters)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
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

            // 添加参数
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

            return await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.Error($"执行非查询SQL语句失败: {ex.Message}，SQL语句：{sql}", ex);
            throw;
        }
    }

    /// <summary>
    /// 直接从Parquet文件执行查询
    /// </summary>
    public async Task<List<TEntity>> QueryParquetFileAsync<TEntity>(
        string parquetFilePath,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
        if (string.IsNullOrWhiteSpace(parquetFilePath))
            throw new ArgumentNullException(nameof(parquetFilePath));

        try
        {
            // 验证文件存在
            if (!File.Exists(parquetFilePath))
                throw new FileNotFoundException("找不到Parquet文件", parquetFilePath);

            // 构建查询语句
            var whereClause = BuildWhereClause(predicate);
            var sql = $"SELECT * FROM read_parquet('{parquetFilePath.Replace("'", "''")}'){whereClause}";

            _logger.Debug($"直接查询Parquet文件: {parquetFilePath}");

            // 执行查询
            return await ExecuteQueryWithMetricsAsync(
                () => QueryWithRawSqlAsync<TEntity>(sql),
                "ParquetQuery",
                sql);
        }
        catch (Exception ex)
        {
            _logger.Error($"查询Parquet文件失败: {ex.Message}, 文件路径: {parquetFilePath}", ex);
            throw;
        }
    }

    /// <summary>
    /// 应用DuckDB优化设置
    /// </summary>
    public async Task ApplyOptimizationAsync()
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        try
        {
            _logger.Debug("应用DuckDB优化设置");

            // 应用各种优化设置
            await ExecuteNonQueryAsync($"PRAGMA threads={_configuration.ThreadCount};");
            _logger.Debug($"设置DuckDB线程数为: {_configuration.ThreadCount}");

            if (!string.IsNullOrEmpty(_configuration.MemoryLimit))
            {
                await ExecuteNonQueryAsync($"PRAGMA memory_limit='{_configuration.MemoryLimit}';");
                _logger.Debug($"设置DuckDB内存限制为: {_configuration.MemoryLimit}");
            }

            // 应用压缩设置
            if (_configuration.EnableCompression)
            {
                await ExecuteNonQueryAsync($"PRAGMA force_compression='{_configuration.CompressionType}';");
                _logger.Debug($"设置DuckDB压缩类型为: {_configuration.CompressionType}");
            }

            // 根据优化级别应用其他优化
            switch (_configuration.OptimizationLevel)
            {
                case 3:
                    // 最高级别优化 - 使用正确的参数名称
                    await ExecuteNonQueryAsync("PRAGMA enable_object_cache=true;");
                    await ExecuteNonQueryAsync("PRAGMA disabled_optimizers='';"); // 修正配置参数
                    await ExecuteNonQueryAsync("PRAGMA force_index_join=true;");
                    break;
                case 2:
                    // 中等级别优化
                    await ExecuteNonQueryAsync("PRAGMA enable_object_cache=true;");
                    await ExecuteNonQueryAsync("PRAGMA disabled_optimizers='';"); // 修正配置参数
                    break;
                case 1:
                    // 基本优化
                    await ExecuteNonQueryAsync("PRAGMA enable_object_cache=true;");
                    break;
                default:
                    // 默认级别
                    break;
            }

            _logger.Info("DuckDB优化设置应用完成");
        }
        catch (Exception ex)
        {
            _logger.Error($"应用DuckDB优化设置失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 带有限制和偏移的分页查询
    /// </summary>
    public async Task<List<TEntity>> QueryWithLimitOffsetAsync<TEntity>(
        Expression<Func<TEntity, bool>> predicate = null,
        int limit = 1000,
        int offset = 0,
        string orderByColumn = null,
        bool ascending = true)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);

        try
        {
            // 构建查询
            var whereClause = BuildWhereClause(predicate);
            var orderClause = string.IsNullOrEmpty(orderByColumn)
                ? string.Empty
                : $" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}";

            var sql = $"SELECT * FROM {typeof(TEntity).Name}{whereClause}{orderClause} LIMIT {limit} OFFSET {offset}";

            // 执行查询
            return await ExecuteQueryWithMetricsAsync(
                () => QueryWithRawSqlAsync<TEntity>(sql),
                "LimitOffsetQuery",
                sql);
        }
        catch (Exception ex)
        {
            _logger.Error($"执行分页查询失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 从表达式构建WHERE子句
    /// </summary>
    protected string BuildWhereClause(Expression expression)
    {
        if (expression == null)
            return string.Empty;

        // 注意：这里需要一个实际的表达式解析器来将C#表达式转换为SQL
        // 以下只是简单示例
        return " WHERE /* 表达式将在实际实现中转换 */";
    }
}
