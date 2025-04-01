using System.Reflection;
using System.Text;

namespace Abp.DuckDB;

/// <summary>
/// 表创建助手类，用于根据实体类生成创建表的 SQL 脚本。
/// </summary>
public static class TableCreationHelper
{
    /// <summary>
    /// 生成创建表的 SQL 脚本。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <returns>创建表的 SQL 脚本。</returns>
    public static string GenerateCreateTableScript<T>()
    {
        Type type = typeof(T);

        // 获取表名
        var tableAttribute = type.GetCustomAttribute<TableAttribute>();
        if (tableAttribute == null)
            throw new InvalidOperationException($"实体类 {type.Name} 缺少 TableAttribute。");

        string tableName = tableAttribute.Name;

        // 构建 CREATE TABLE 语句
        StringBuilder sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE IF NOT EXISTS {tableName} (");

        // 获取属性列表
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        List<string> columnDefinitions = new List<string>();

        foreach (var prop in properties)
        {
            // 跳过被标记为 [NotMapped] 的属性
            if (Attribute.IsDefined(prop, typeof(NotMappedAttribute)))
                continue;

            var columnAttribute = prop.GetCustomAttribute<ColumnAttribute>();

            // 自动推断列名和数据类型
            string columnName = columnAttribute?.Name ?? prop.Name;
            string dataType = columnAttribute?.DataType ?? GetDuckDBDataType(prop.PropertyType);

            // 判断是否为主键和其他属性
            bool isPrimaryKey = columnAttribute?.IsPrimaryKey ?? false;
            bool isNullable = columnAttribute?.IsNullable ?? IsNullableType(prop);
            bool isAutoIncrement = columnAttribute?.IsAutoIncrement ?? false;
            string defaultValue = columnAttribute?.DefaultValue;

            var columnDef = BuildColumnDefinition(columnName, dataType, isPrimaryKey, isNullable, isAutoIncrement, defaultValue);
            columnDefinitions.Add(columnDef);
        }

        sb.AppendLine(string.Join(",\n", columnDefinitions));
        sb.AppendLine(");");

        // 如果有自增主键，生成序列（根据数据库的需要）
        string sequenceScript = GenerateSequenceScript<T>(properties);

        return sequenceScript + sb.ToString();
    }

    /// <summary>
    /// 构建列的定义字符串。
    /// </summary>
    private static string BuildColumnDefinition(string columnName, string dataType, bool isPrimaryKey, bool isNullable, bool isAutoIncrement, string defaultValue)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"{columnName} {dataType}");

        if (!isNullable)
            sb.Append(" NOT NULL");

        if (isPrimaryKey)
            sb.Append(" PRIMARY KEY");

        if (isAutoIncrement)
        {
            string sequenceName = $"seq_{columnName.ToLower()}";
            sb.Append($" DEFAULT nextval('{sequenceName}')");
        }

        if (!string.IsNullOrEmpty(defaultValue))
            sb.Append($" DEFAULT {defaultValue}");

        return sb.ToString();
    }

    /// <summary>
    /// 生成序列的创建脚本（用于自增主键）。
    /// </summary>
    private static string GenerateSequenceScript<T>(PropertyInfo[] properties)
    {
        var primaryKeyProperty = properties.FirstOrDefault(p =>
            (p.GetCustomAttribute<ColumnAttribute>()?.IsPrimaryKey ?? false) &&
            (p.GetCustomAttribute<ColumnAttribute>()?.IsAutoIncrement ?? false));

        if (primaryKeyProperty == null)
            return string.Empty;

        var columnAttribute = primaryKeyProperty.GetCustomAttribute<ColumnAttribute>();
        string columnName = columnAttribute?.Name ?? primaryKeyProperty.Name;
        string sequenceName = $"seq_{columnName.ToLower()}";

        return $"CREATE SEQUENCE IF NOT EXISTS {sequenceName} START 1;\n";
    }

    /// <summary>
    /// 将 C# 类型映射为 DuckDB 数据类型。
    /// </summary>
    private static string GetDuckDBDataType(Type type)
    {
        if (type == typeof(int) || type == typeof(int?))
            return "INTEGER";
        if (type == typeof(long) || type == typeof(long?))
            return "BIGINT";
        if (type == typeof(string))
            return "TEXT";
        if (type == typeof(DateTime) || type == typeof(DateTime?))
            return "TIMESTAMP";
        if (type == typeof(bool) || type == typeof(bool?))
            return "BOOLEAN";
        if (type == typeof(double) || type == typeof(double?))
            return "DOUBLE";
        
        // 根据需要添加更多类型映射
        throw new NotSupportedException($"类型 {type.Name} 不支持自动映射为 DuckDB 数据类型。");
    }

    /// <summary>
    /// 判断属性是否可为空。
    /// </summary>
    private static bool IsNullableType(PropertyInfo prop)
    {
        if (!prop.PropertyType.IsValueType)
            return true; // 引用类型默认可为空

        if (Nullable.GetUnderlyingType(prop.PropertyType) != null)
            return true; // 可空值类型
        
        return false; // 值类型不可为空
    }
}
