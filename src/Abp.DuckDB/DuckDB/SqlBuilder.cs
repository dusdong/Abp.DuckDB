using System.Linq.Expressions;
using System.Text;
using Castle.Core.Logging;

namespace Abp.DuckDB;

/// <summary>
/// DuckDB SQL查询构建器类，负责生成所有SQL语句
/// </summary>
public class SqlBuilder
{
    private readonly ILogger _logger;
    private readonly SqlExpressionVisitor _expressionVisitor;

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="logger">日志记录器</param>
    public SqlBuilder(ILogger logger)
    {
        _logger = logger ?? NullLogger.Instance;
        _expressionVisitor = new SqlExpressionVisitor(_logger);
    }

    /// <summary>
    /// 从表达式构建WHERE子句
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="expression">Lambda表达式</param>
    /// <returns>WHERE子句（不含WHERE关键字）</returns>
    public string BuildWhereClause<TEntity>(Expression<Func<TEntity, bool>> expression)
    {
        if (expression == null)
            return string.Empty;

        try
        {
            return _expressionVisitor.Translate(expression);
        }
        catch (Exception ex)
        {
            _logger.Error($"构建WHERE子句失败: {ex.Message}", ex);
            throw new DuckDBOperationException("无法将表达式转换为SQL语句", ex);
        }
    }

    /// <summary>
    /// 构建Parquet数据源子句
    /// </summary>
    /// <param name="filePaths">文件路径集合</param>
    /// <returns>数据源SQL片段</returns>
    public string BuildParquetSourceClause(IEnumerable<string> filePaths)
    {
        if (filePaths == null || !filePaths.Any())
            throw new ArgumentException("必须提供至少一个文件路径", nameof(filePaths));

        // 使用文件名生成SQL
        if (filePaths.Count() == 1)
        {
            // 单文件情况
            return $"read_parquet('{EscapePath(filePaths.First())}')";
        }
        else
        {
            // 对于大型文件集合使用优化策略
            if (filePaths.Count() > 1000)
            {
                _logger.Warn($"大量文件路径（{filePaths.Count()}）可能导致性能问题");
            }

            // 多文件情况，使用列表语法和StringBuilder优化大量文件情况
            var sb = new StringBuilder();
            sb.Append("read_parquet([");

            bool isFirst = true;
            foreach (var path in filePaths)
            {
                if (!isFirst)
                    sb.Append(", ");

                sb.Append($"'{EscapePath(path)}'");
                isFirst = false;
            }

            sb.Append("])");
            return sb.ToString();
        }
    }

    /// <summary>
    /// 构建直接的Parquet查询
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="filePaths">文件路径集合</param>
    /// <param name="predicate">过滤条件表达式</param>
    /// <param name="selectedColumns">要查询的列，为null则查询所有列</param>
    /// <returns>完整SQL查询</returns>
    public string BuildDirectParquetQuery<TEntity>(
        IEnumerable<string> filePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        IEnumerable<string> selectedColumns = null)
    {
        string whereClause = string.Empty;
        if (predicate != null)
        {
            whereClause = BuildWhereClause(predicate);
        }

        return BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);
    }

    /// <summary>
    /// 构建直接的Parquet查询
    /// </summary>
    /// <param name="filePaths">文件路径集合</param>
    /// <param name="whereClause">WHERE子句（不含WHERE关键字）</param>
    /// <param name="selectedColumns">要查询的列，为null则查询所有列</param>
    /// <returns>完整SQL查询</returns>
    public string BuildDirectParquetQuery(
        IEnumerable<string> filePaths,
        string whereClause,
        IEnumerable<string> selectedColumns = null)
    {
        // 确定查询的列
        string columns = "*";
        if (selectedColumns != null && selectedColumns.Any())
        {
            columns = string.Join(", ", selectedColumns);
        }

        // 构建查询
        var sb = new StringBuilder();
        sb.Append($"SELECT {columns} FROM {BuildParquetSourceClause(filePaths)}");

        // 添加WHERE子句（如果有）
        if (!string.IsNullOrEmpty(whereClause))
        {
            sb.Append($" WHERE {whereClause}");
        }

        string sql = sb.ToString();
        _logger.Debug($"生成SQL查询: {TruncateSql(sql)}");
        return sql;
    }

    /// <summary>
    /// 构建直接的Parquet计数查询
    /// </summary>
    /// <param name="filePaths">文件路径集合</param>
    /// <param name="whereClause">WHERE子句（不含WHERE关键字）</param>
    /// <returns>完整计数SQL查询</returns>
    public string BuildDirectParquetCountQuery(IEnumerable<string> filePaths, string whereClause)
    {
        var sb = new StringBuilder();
        sb.Append($"SELECT COUNT(*) FROM {BuildParquetSourceClause(filePaths)}");

        // 添加WHERE子句（如果有）
        if (!string.IsNullOrEmpty(whereClause))
        {
            sb.Append($" WHERE {whereClause}");
        }

        string sql = sb.ToString();
        _logger.Debug($"生成计数SQL查询: {TruncateSql(sql)}");
        return sql;
    }

    /// <summary>
    /// 构建聚合查询
    /// </summary>
    /// <param name="filePaths">文件路径集合</param>
    /// <param name="aggregateFunction">聚合函数表达式，如 "SUM(amount)"</param>
    /// <param name="whereClause">WHERE子句（不含WHERE关键字）</param>
    /// <returns>完整聚合SQL查询</returns>
    public string BuildDirectParquetAggregateQuery(
        IEnumerable<string> filePaths,
        string aggregateFunction,
        string whereClause)
    {
        var sb = new StringBuilder();
        sb.Append($"SELECT {aggregateFunction} FROM {BuildParquetSourceClause(filePaths)}");

        // 添加WHERE子句（如果有）
        if (!string.IsNullOrEmpty(whereClause))
        {
            sb.Append($" WHERE {whereClause}");
        }

        string sql = sb.ToString();
        _logger.Debug($"生成聚合SQL查询: {TruncateSql(sql)}");
        return sql;
    }

    /// <summary>
    /// 构建分页查询
    /// </summary>
    /// <param name="filePaths">文件路径集合</param>
    /// <param name="whereClause">WHERE子句（不含WHERE关键字）</param>
    /// <param name="selectedColumns">要查询的列，为null则查询所有列</param>
    /// <param name="orderByColumn">排序列</param>
    /// <param name="ascending">是否升序</param>
    /// <param name="limit">限制记录数</param>
    /// <param name="offset">跳过记录数</param>
    /// <returns>完整分页SQL查询</returns>
    public string BuildDirectParquetPagedQuery(
        IEnumerable<string> filePaths,
        string whereClause,
        IEnumerable<string> selectedColumns,
        string orderByColumn,
        bool ascending,
        int limit,
        int offset)
    {
        // 构建基本查询
        var sql = BuildDirectParquetQuery(filePaths, whereClause, selectedColumns);
        var sb = new StringBuilder(sql);

        // 添加排序
        if (!string.IsNullOrEmpty(orderByColumn))
        {
            sb.Append($" ORDER BY {orderByColumn} {(ascending ? "ASC" : "DESC")}");
        }

        // 添加分页
        sb.Append($" LIMIT {limit} OFFSET {offset}");

        string finalSql = sb.ToString();
        _logger.Debug($"生成分页SQL查询: LIMIT={limit}, OFFSET={offset}");
        return finalSql;
    }

    /// <summary>
    /// 构建向量化过滤查询
    /// </summary>
    public string BuildVectorizedFilterQuery(
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
                FormatValueForSql(sb, value);
            }

            sb.Append(")");
        }

        // 添加限制
        if (resultLimit > 0)
        {
            sb.Append($" LIMIT {resultLimit}");
        }

        string sql = sb.ToString();
        _logger.Debug($"生成向量化过滤查询: 表={tableName}, 列数={columns.Length}");
        return sql;
    }

    /// <summary>
    /// 格式化SQL值
    /// </summary>
    private void FormatValueForSql(StringBuilder sb, object value)
    {
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
        else if (value is bool boolValue)
        {
            sb.Append(boolValue ? "TRUE" : "FALSE");
        }
        else
        {
            sb.Append(value.ToString());
        }
    }

    /// <summary>
    /// 转义路径中的特殊字符
    /// </summary>
    /// <param name="path">文件路径</param>
    /// <returns>转义后的路径</returns>
    private string EscapePath(string path)
    {
        // 避免SQL注入和特殊字符问题
        return path?.Replace("'", "''");
    }

    /// <summary>
    /// 截断SQL语句用于日志记录
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="maxLength">最大长度</param>
    /// <returns>截断后的SQL</returns>
    private string TruncateSql(string sql, int maxLength = 200)
    {
        if (string.IsNullOrEmpty(sql)) return string.Empty;
        return sql.Length <= maxLength ? sql : sql.Substring(0, maxLength) + "...";
    }
}
