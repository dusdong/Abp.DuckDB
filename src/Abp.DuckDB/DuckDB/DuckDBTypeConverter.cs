using System.ComponentModel;
using System.Numerics;
using System.Reflection;
using Castle.Core.Logging;

namespace Abp.DuckDB;

/// <summary>
/// DuckDB类型转换工具类，提供数据类型之间的安全转换功能
/// </summary>
public static class DuckDBTypeConverter
{
    /// <summary>
    /// 安全地将对象转换为指定类型
    /// </summary>
    /// <typeparam name="TResult">目标类型</typeparam>
    /// <param name="value">要转换的值</param>
    /// <param name="logger">日志记录器，可选</param>
    /// <returns>转换后的值</returns>
    public static TResult SafeConvert<TResult>(object value, ILogger logger = null)
    {
        if (value == null || value == DBNull.Value)
            return default!;

        Type targetType = typeof(TResult);

        try
        {
            // 处理可空类型
            var nullableType = Nullable.GetUnderlyingType(targetType);
            if (nullableType != null)
            {
                targetType = nullableType;
            }

            // 处理特定类型转换
            if (targetType.IsEnum)
            {
                return (TResult)ConvertToEnum(value, targetType);
            }

            if (IsNumericType(targetType))
            {
                return (TResult)ConvertToNumericType(value, targetType);
            }

            if (IsDateTimeType(targetType))
            {
                return (TResult)ConvertToDateTimeType(value, targetType);
            }

            if (targetType == typeof(string))
            {
                return (TResult)(object)value.ToString();
            }

            if (targetType == typeof(Guid) || targetType == typeof(Guid?))
            {
                return (TResult)ConvertToGuid(value);
            }

            if (targetType == typeof(bool) || targetType == typeof(bool?))
            {
                return (TResult)ConvertToBoolean(value);
            }

            // 通用转换
            return (TResult)ConvertGenericType(value, targetType, logger);
        }
        catch (Exception ex)
        {
            logger?.Error($"类型转换失败: {ex.Message}, 源类型: {value.GetType().Name}, 目标类型: {targetType.Name}", ex);

            throw new InvalidCastException($"无法将 '{value.GetType().Name}' 转换为 {targetType.Name}", ex);
        }
    }

    /// <summary>
    /// 检查类型是否为数字类型
    /// </summary>
    private static bool IsNumericType(Type type)
    {
        return type == typeof(byte) || type == typeof(sbyte) ||
               type == typeof(short) || type == typeof(ushort) ||
               type == typeof(int) || type == typeof(uint) ||
               type == typeof(long) || type == typeof(ulong) ||
               type == typeof(float) || type == typeof(double) ||
               type == typeof(decimal) || type == typeof(BigInteger);
    }

    /// <summary>
    /// 检查类型是否为日期时间类型
    /// </summary>
    private static bool IsDateTimeType(Type type)
    {
        return type == typeof(DateTime) || type == typeof(DateTimeOffset) ||
               type == typeof(DateOnly) || type == typeof(TimeOnly);
    }

    /// <summary>
    /// 将值转换为枚举类型
    /// </summary>
    private static object ConvertToEnum(object value, Type enumType)
    {
        if (value is string strValue)
        {
            return Enum.Parse(enumType, strValue);
        }

        if (value is int || value is long || value is byte || value is short)
        {
            return Enum.ToObject(enumType, value);
        }

        // 尝试转换其他类型值
        var underlyingValue = Convert.ChangeType(value, Enum.GetUnderlyingType(enumType));
        return Enum.ToObject(enumType, underlyingValue);
    }

    /// <summary>
    /// 将值转换为数值类型
    /// </summary>
    private static object ConvertToNumericType(object value, Type targetType)
    {
        // 处理BigInteger作为源类型的情况
        if (value is BigInteger bigIntValue)
        {
            if (targetType == typeof(int))
            {
                if (bigIntValue <= int.MaxValue && bigIntValue >= int.MinValue)
                    return (int)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出Int32范围。");
            }

            if (targetType == typeof(long))
            {
                if (bigIntValue <= long.MaxValue && bigIntValue >= long.MinValue)
                    return (long)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出Int64范围。");
            }

            if (targetType == typeof(decimal))
            {
                if (bigIntValue <= (BigInteger)decimal.MaxValue && bigIntValue >= (BigInteger)decimal.MinValue)
                    return (decimal)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出Decimal范围。");
            }

            if (targetType == typeof(double))
                return (double)bigIntValue;

            if (targetType == typeof(float))
                return (float)bigIntValue;

            if (targetType == typeof(uint))
            {
                if (bigIntValue <= uint.MaxValue && bigIntValue >= uint.MinValue)
                    return (uint)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出UInt32范围。");
            }

            if (targetType == typeof(ulong))
            {
                if (bigIntValue <= ulong.MaxValue && bigIntValue >= ulong.MinValue)
                    return (ulong)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出UInt64范围。");
            }

            if (targetType == typeof(byte))
            {
                if (bigIntValue <= byte.MaxValue && bigIntValue >= byte.MinValue)
                    return (byte)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出Byte范围。");
            }

            if (targetType == typeof(sbyte))
            {
                if (bigIntValue <= sbyte.MaxValue && bigIntValue >= sbyte.MinValue)
                    return (sbyte)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出SByte范围。");
            }

            if (targetType == typeof(short))
            {
                if (bigIntValue <= short.MaxValue && bigIntValue >= short.MinValue)
                    return (short)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出Int16范围。");
            }

            if (targetType == typeof(ushort))
            {
                if (bigIntValue <= ushort.MaxValue && bigIntValue >= ushort.MinValue)
                    return (ushort)bigIntValue;
                throw new OverflowException($"BigInteger值 {bigIntValue} 超出UInt16范围。");
            }

            // 如果目标类型也是BigInteger，直接返回
            if (targetType == typeof(BigInteger))
                return bigIntValue;
        }

        // 处理BigInteger作为目标类型的情况
        if (targetType == typeof(BigInteger))
        {
            if (value is int intValue)
                return new BigInteger(intValue);
            if (value is long longValue)
                return new BigInteger(longValue);
            if (value is decimal decimalValue)
                return new BigInteger(decimalValue);
            if (value is double doubleValue)
                return new BigInteger(doubleValue);
            if (value is string strValue)
                return BigInteger.Parse(strValue);
        }

        // 原有的转换逻辑
        if (targetType == typeof(int)) return Convert.ToInt32(value);
        if (targetType == typeof(long)) return Convert.ToInt64(value);
        if (targetType == typeof(decimal)) return Convert.ToDecimal(value);
        if (targetType == typeof(double)) return Convert.ToDouble(value);
        if (targetType == typeof(float)) return Convert.ToSingle(value);
        if (targetType == typeof(byte)) return Convert.ToByte(value);
        if (targetType == typeof(sbyte)) return Convert.ToSByte(value);
        if (targetType == typeof(short)) return Convert.ToInt16(value);
        if (targetType == typeof(ushort)) return Convert.ToUInt16(value);
        if (targetType == typeof(uint)) return Convert.ToUInt32(value);
        if (targetType == typeof(ulong)) return Convert.ToUInt64(value);

        // 默认转换
        return Convert.ChangeType(value, targetType);
    }

    /// <summary>
    /// 将值转换为日期时间类型
    /// </summary>
    private static object ConvertToDateTimeType(object value, Type targetType)
    {
        if (value is string dateString)
        {
            if (targetType == typeof(DateTime))
                return DateTime.Parse(dateString);
            if (targetType == typeof(DateTimeOffset))
                return DateTimeOffset.Parse(dateString);
            if (targetType == typeof(DateOnly))
                return DateOnly.Parse(dateString);
            if (targetType == typeof(TimeOnly))
                return TimeOnly.Parse(dateString);
        }

        // 处理从 DateTime 到其他日期时间类型的转换
        if (value is DateTime dateTime)
        {
            if (targetType == typeof(DateTimeOffset))
                return new DateTimeOffset(dateTime);
            if (targetType == typeof(DateOnly))
                return DateOnly.FromDateTime(dateTime);
            if (targetType == typeof(TimeOnly))
                return TimeOnly.FromDateTime(dateTime);
        }

        // 处理从 DateTimeOffset 到其他日期时间类型的转换
        if (value is DateTimeOffset dateTimeOffset)
        {
            if (targetType == typeof(DateTime))
                return dateTimeOffset.DateTime;
            if (targetType == typeof(DateOnly))
                return DateOnly.FromDateTime(dateTimeOffset.DateTime);
            if (targetType == typeof(TimeOnly))
                return TimeOnly.FromDateTime(dateTimeOffset.DateTime);
        }

        // 默认转换
        return Convert.ChangeType(value, targetType);
    }

    /// <summary>
    /// 将值转换为Guid
    /// </summary>
    private static object ConvertToGuid(object value)
    {
        if (value is string strValue)
            return Guid.Parse(strValue);
        if (value is byte[] bytes)
            return new Guid(bytes);
        return value; // 假设它已经是一个Guid
    }

    /// <summary>
    /// 将值转换为布尔值
    /// </summary>
    private static object ConvertToBoolean(object value)
    {
        if (value is long longValue)
            return longValue != 0;
        if (value is int intValue)
            return intValue != 0;
        if (value is string strValue)
        {
            if (string.Equals(strValue, "true", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(strValue, "1"))
                return true;
            if (string.Equals(strValue, "false", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(strValue, "0"))
                return false;
        }

        return Convert.ToBoolean(value);
    }

    /// <summary>
    /// 通用类型转换
    /// </summary>
    private static object ConvertGenericType(object value, Type targetType, ILogger logger)
    {
        // 如果原始类型和目标类型相同，或目标类型是原始类型的基类
        if (value.GetType() == targetType || targetType.IsAssignableFrom(value.GetType()))
            return value;

        // 尝试使用TypeDescriptor进行转换
        var converter = TypeDescriptor.GetConverter(value.GetType());
        if (converter != null && converter.CanConvertTo(targetType))
            return converter.ConvertTo(value, targetType);

        converter = TypeDescriptor.GetConverter(targetType);
        if (converter != null && converter.CanConvertFrom(value.GetType()))
            return converter.ConvertFrom(value);

        // 最后尝试使用Convert.ChangeType
        if (typeof(IConvertible).IsAssignableFrom(targetType))
            return Convert.ChangeType(value, targetType);

        // 记录警告并返回默认值
        logger?.Warn($"无法将类型 {value.GetType().Name} 转换为 {targetType.Name}，返回默认值");
        return null;
    }

    /// <summary>
    /// 安全转换对象为指定类型
    /// </summary>
    /// <param name="value">要转换的值</param>
    /// <param name="targetType">目标类型</param>
    /// <param name="logger">日志记录器</param>
    /// <returns>转换后的值</returns>
    public static object SafeConvert(object value, Type targetType, ILogger logger = null)
    {
        if (value == null || value == DBNull.Value)
        {
            return null;
        }

        Type underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        try
        {
            // 处理特定类型
            if (underlyingType.IsEnum)
            {
                return ConvertToEnum(value, underlyingType);
            }

            if (IsNumericType(underlyingType))
            {
                return ConvertToNumericType(value, underlyingType);
            }

            if (IsDateTimeType(underlyingType))
            {
                return ConvertToDateTimeType(value, underlyingType);
            }

            if (underlyingType == typeof(Guid))
            {
                return ConvertToGuid(value);
            }

            if (underlyingType == typeof(bool))
            {
                return ConvertToBoolean(value);
            }

            if (underlyingType == typeof(string))
            {
                return value.ToString();
            }

            if (underlyingType == typeof(byte[]) && value is string base64String)
            {
                return Convert.FromBase64String(base64String);
            }

            // 通用转换
            return ConvertGenericType(value, underlyingType, logger) ??
                   Convert.ChangeType(value, underlyingType);
        }
        catch (Exception ex)
        {
            logger?.Error($"转换值 '{value}' ({value?.GetType().Name}) 到类型 '{targetType.Name}' 失败: {ex.Message}", ex);
            
            throw new InvalidCastException($"无法将 '{value}' 转换为 {targetType.Name}", ex);
        }
    }

    /// <summary>
    /// 设置实体属性值，包含类型转换处理
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="entity">实体实例</param>
    /// <param name="property">要设置的属性</param>
    /// <param name="value">属性值</param>
    /// <param name="logger">日志记录器，可选</param>
    public static void SetPropertyValue<TEntity>(TEntity entity, PropertyInfo property, object value, ILogger logger = null)
    {
        if (entity == null || property == null || value == DBNull.Value || value == null)
            return;

        try
        {
            var targetType = property.PropertyType;

            // 使用SafeConvert处理转换
            object convertedValue = null;

            // 处理可空类型
            var nullableType = Nullable.GetUnderlyingType(targetType);
            if (nullableType != null)
            {
                convertedValue = SafeConvert(value, nullableType, logger);
            }
            else
            {
                convertedValue = SafeConvert(value, targetType, logger);
            }

            if (convertedValue != null)
            {
                property.SetValue(entity, convertedValue);
            }
        }
        catch (Exception ex)
        {
            logger?.Error($"设置属性 {property.Name} 值时发生异常: {ex.Message}", ex);

            throw new InvalidOperationException($"无法设置属性 '{property.Name}' 的值", ex);
        }
    }
}
