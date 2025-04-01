namespace Abp.DuckDB;

/// <summary>
/// 用于指定数据库表名的特性。
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class TableAttribute : Attribute
{
    /// <summary>
    /// 表名。
    /// </summary>
    public string Name { get; }

    public TableAttribute(string name) => Name = name;
}

/// <summary>
/// 用于指定数据库列名、数据类型等信息的特性。
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class ColumnAttribute : Attribute
{
    /// <summary>
    /// 列名。
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// 数据类型。
    /// </summary>
    public string DataType { get; }

    /// <summary>
    /// 是否为主键。
    /// </summary>
    public bool IsPrimaryKey { get; set; } = false;

    /// <summary>
    /// 是否可为空。
    /// </summary>
    public bool IsNullable { get; set; } = true;

    /// <summary>
    /// 是否自增。
    /// </summary>
    public bool IsAutoIncrement { get; set; } = false;

    /// <summary>
    /// 默认值。
    /// </summary>
    public string DefaultValue { get; set; } = null;

    public ColumnAttribute(string name, string dataType)
    {
        Name = name;
        DataType = dataType;
    }
}

/// <summary>
/// 不应映射到数据库
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class NotMappedAttribute : Attribute
{
}
