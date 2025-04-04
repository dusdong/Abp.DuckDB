using System.Reflection;

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
    
    /// <summary>
    /// 获取类型的基础类型（如果是可空类型）
    /// </summary>
    public static Type GetUnderlyingType(this Type type)
    {
        return Nullable.GetUnderlyingType(type) ?? type;
    }
    
    /// <summary>
    /// 检查是否可以将源类型转换为目标类型
    /// </summary>
    public static bool CanConvertTo(this Type sourceType, Type targetType)
    {
        // 获取基础类型
        var underlyingTargetType = targetType.GetUnderlyingType();
        var underlyingSourceType = sourceType.GetUnderlyingType();
        
        // 相同类型直接可转换
        if (underlyingTargetType == underlyingSourceType)
            return true;
            
        // 数值类型转换
        if (IsNumericType(underlyingSourceType) && IsNumericType(underlyingTargetType))
            return true;
            
        // 检查是否有隐式转换
        var methods = underlyingTargetType
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "op_Implicit" && m.ReturnType == underlyingTargetType);
            
        foreach (var method in methods)
        {
            var parameters = method.GetParameters();
            if (parameters.Length == 1 && parameters[0].ParameterType == underlyingSourceType)
                return true;
        }
        
        return false;
    }
    
    /// <summary>
    /// 检查是否为数值类型
    /// </summary>
    public static bool IsNumericType(this Type type)
    {
        switch (Type.GetTypeCode(type))
        {
            case TypeCode.Byte:
            case TypeCode.SByte:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
            case TypeCode.UInt64:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.Int64:
            case TypeCode.Decimal:
            case TypeCode.Double:
            case TypeCode.Single:
                return true;
            default:
                return false;
        }
    }
}
