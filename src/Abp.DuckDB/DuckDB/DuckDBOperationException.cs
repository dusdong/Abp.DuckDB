namespace Abp.DuckDB;

/// <summary>
/// DuckDB操作异常类，用于包装与DuckDB相关的所有异常
/// </summary>
public class DuckDBOperationException : Exception
{
    /// <summary>
    /// 操作类型
    /// </summary>
    public string OperationType { get; }

    /// <summary>
    /// SQL语句（如果适用）
    /// </summary>
    public string Sql { get; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误信息</param>
    public DuckDBOperationException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误信息</param>
    /// <param name="innerException">内部异常</param>
    public DuckDBOperationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="operationType">操作类型</param>
    /// <param name="message">错误信息</param>
    /// <param name="innerException">内部异常</param>
    public DuckDBOperationException(string operationType, string message, Exception innerException)
        : base(message, innerException)
    {
        OperationType = operationType;
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="operationType">操作类型</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="message">错误信息</param>
    /// <param name="innerException">内部异常</param>
    public DuckDBOperationException(string operationType, string sql, string message, Exception innerException)
        : base(message, innerException)
    {
        OperationType = operationType;
        Sql = sql;
    }
}
