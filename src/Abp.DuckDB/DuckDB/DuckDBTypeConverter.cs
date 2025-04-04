using System.ComponentModel;
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

            // 处理枚举类型
            if (targetType.IsEnum)
            {
                if (value is string strValue)
                {
                    return (TResult)Enum.Parse(targetType, strValue);
                }
                else if (value is int || value is long || value is byte || value is short)
                {
                    return (TResult)Enum.ToObject(targetType, value);
                }
            }

            // 处理常见的数值类型
            if (targetType == typeof(int) || targetType == typeof(int?))
                return (TResult)(object)Convert.ToInt32(value);

            if (targetType == typeof(long) || targetType == typeof(long?))
                return (TResult)(object)Convert.ToInt64(value);

            if (targetType == typeof(decimal) || targetType == typeof(decimal?))
                return (TResult)(object)Convert.ToDecimal(value);

            if (targetType == typeof(double) || targetType == typeof(double?))
                return (TResult)(object)Convert.ToDouble(value);

            if (targetType == typeof(float) || targetType == typeof(float?))
                return (TResult)(object)Convert.ToSingle(value);

            if (targetType == typeof(bool) || targetType == typeof(bool?))
                return (TResult)(object)Convert.ToBoolean(value);

            if (targetType == typeof(DateTime) || targetType == typeof(DateTime?))
                return (TResult)(object)Convert.ToDateTime(value);

            if (targetType == typeof(Guid) || targetType == typeof(Guid?))
            {
                if (value is string strValue)
                    return (TResult)(object)Guid.Parse(strValue);
                return (TResult)(object)value;
            }

            // 如果是字符串，直接转换
            if (targetType == typeof(string))
                return (TResult)(object)value.ToString();

            // 如果目标类型实现了IConvertible接口，使用正常的转换
            if (typeof(IConvertible).IsAssignableFrom(targetType))
                return (TResult)Convert.ChangeType(value, targetType);

            // 原始类型和类型兼容的情况
            if (value.GetType() == targetType || targetType.IsAssignableFrom(value.GetType()))
                return (TResult)value;

            // 尝试使用反射进行转换
            var converter = TypeDescriptor.GetConverter(value.GetType());
            if (converter != null && converter.CanConvertTo(targetType))
                return (TResult)converter.ConvertTo(value, targetType);

            converter = TypeDescriptor.GetConverter(targetType);
            if (converter != null && converter.CanConvertFrom(value.GetType()))
                return (TResult)converter.ConvertFrom(value);

            // 记录警告并返回默认值
            logger?.Warn($"无法将类型 {value.GetType().Name} 转换为 {targetType.Name}，返回默认值");
            return default!;
        }
        catch (Exception ex)
        {
            logger?.Error($"类型转换失败: {ex.Message}, 源类型: {value.GetType().Name}, 目标类型: {targetType.Name}", ex);
            return default!;
        }
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
            // 处理特殊类型
            if (underlyingType == typeof(Guid) && value is string guidString)
            {
                return Guid.Parse(guidString);
            }
            else if (underlyingType == typeof(DateTime) && value is string dateString)
            {
                return DateTime.Parse(dateString);
            }
            else if (underlyingType.IsEnum)
            {
                if (value is string enumString)
                {
                    return Enum.Parse(underlyingType, enumString, true);
                }
                else
                {
                    return Enum.ToObject(underlyingType, value);
                }
            }
            else if (underlyingType == typeof(bool) && value is long longValue)
            {
                return longValue != 0;
            }
            else if (underlyingType == typeof(byte[]) && value is string base64String)
            {
                return Convert.FromBase64String(base64String);
            }

            // 标准转换
            return Convert.ChangeType(value, underlyingType);
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

            // 处理可空类型
            var nullableType = Nullable.GetUnderlyingType(targetType);
            if (nullableType != null)
            {
                targetType = nullableType;
            }

            // 处理枚举类型
            if (targetType.IsEnum)
            {
                if (value is string strValue)
                {
                    property.SetValue(entity, Enum.Parse(targetType, strValue));
                }
                else
                {
                    property.SetValue(entity, Enum.ToObject(targetType, value));
                }

                return;
            }

            // 处理常见类型转换
            if (value is decimal decValue && targetType == typeof(double))
            {
                property.SetValue(entity, Convert.ToDouble(decValue));
                return;
            }

            if (value is long longValue && targetType == typeof(int))
            {
                property.SetValue(entity, Convert.ToInt32(longValue));
                return;
            }

            // 使用TypeDescriptor进行高级转换
            TypeConverter converter = TypeDescriptor.GetConverter(targetType);
            if (converter != null && converter.CanConvertFrom(value.GetType()))
            {
                property.SetValue(entity, converter.ConvertFrom(value));
                return;
            }

            // 通用转换
            property.SetValue(entity, Convert.ChangeType(value, targetType));
        }
        catch (Exception ex)
        {
            logger?.Error($"设置属性 {property.Name} 值时发生异常: {ex.Message}", ex);
            // 不抛出异常，继续处理后续属性
        }
    }
}
