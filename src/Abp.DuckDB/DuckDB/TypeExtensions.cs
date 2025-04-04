namespace Abp.DuckDB;

/// <summary>
/// 类型扩展方法
/// </summary>
public static class TypeExtensions
{
    /// <summary>
    /// 检查类型是否为可空类型
    /// </summary>
    public static bool IsNullable(this Type type)
    {
        return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }
}
