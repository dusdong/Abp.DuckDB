using System.Collections.Concurrent;
using System.Reflection;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB查询元数据缓存
/// </summary>
/// <summary>
/// DuckDB查询元数据缓存
/// </summary>
internal static class DuckDBMetadataCache
{
    // 默认缓存过期时间(30分钟)
    private static readonly TimeSpan DEFAULT_EXPIRATION = TimeSpan.FromMinutes(30);

    // 缓存包装类，用于存储缓存项和过期时间
    private class CacheItem<T>
    {
        public T Value { get; set; }
        public DateTime ExpirationTime { get; set; }

        public bool IsExpired => DateTime.Now > ExpirationTime;

        public CacheItem(T value, TimeSpan expiration)
        {
            Value = value;
            ExpirationTime = DateTime.Now.Add(expiration);
        }
    }

    // 缓存实体属性信息
    private static readonly ConcurrentDictionary<Type, CacheItem<PropertyInfo[]>> TypePropertiesCache =
        new ConcurrentDictionary<Type, CacheItem<PropertyInfo[]>>();

    // 缓存实体列名列表
    private static readonly ConcurrentDictionary<Type, CacheItem<List<string>>> EntityColumnsCache =
        new ConcurrentDictionary<Type, CacheItem<List<string>>>();

    // 缓存属性到列名的映射
    private static readonly ConcurrentDictionary<Type, CacheItem<Dictionary<string, string>>> PropertyColumnMappingsCache =
        new ConcurrentDictionary<Type, CacheItem<Dictionary<string, string>>>();

    // 缓存表达式转SQL结果
    private static readonly ConcurrentDictionary<string, CacheItem<string>> ExpressionSqlCache =
        new ConcurrentDictionary<string, CacheItem<string>>();

    // 缓存统计 
    private static int _propertyHits;
    private static int _columnHits;
    private static int _expressionHits;
    private static int _expirations;

    // 获取或添加类型的属性信息
    public static PropertyInfo[] GetOrAddProperties(Type type, Func<Type, PropertyInfo[]> factory)
    {
        if (TypePropertiesCache.TryGetValue(type, out var cacheItem))
        {
            // 检查是否过期
            if (!cacheItem.IsExpired)
            {
                Interlocked.Increment(ref _propertyHits);
                return cacheItem.Value;
            }
            else
            {
                // 过期则移除
                TypePropertiesCache.TryRemove(type, out _);
                Interlocked.Increment(ref _expirations);
            }
        }

        // 创建新缓存项
        var properties = factory(type);
        TypePropertiesCache[type] = new CacheItem<PropertyInfo[]>(properties, DEFAULT_EXPIRATION);
        return properties;
    }

    // 获取或添加列名列表
    public static List<string> GetOrAddEntityColumns(Type type, Func<Type, List<string>> factory)
    {
        if (EntityColumnsCache.TryGetValue(type, out var cacheItem))
        {
            // 检查是否过期
            if (!cacheItem.IsExpired)
            {
                Interlocked.Increment(ref _columnHits);
                return cacheItem.Value;
            }
            else
            {
                // 过期则移除
                EntityColumnsCache.TryRemove(type, out _);
                Interlocked.Increment(ref _expirations);
            }
        }

        // 创建新缓存项
        var columns = factory(type);
        EntityColumnsCache[type] = new CacheItem<List<string>>(columns, DEFAULT_EXPIRATION);
        return columns;
    }

    // 获取或添加表达式的SQL转换
    public static string GetOrAddExpressionSql(string expressionKey, Func<string, string> factory)
    {
        if (ExpressionSqlCache.TryGetValue(expressionKey, out var cacheItem))
        {
            // 检查是否过期
            if (!cacheItem.IsExpired)
            {
                Interlocked.Increment(ref _expressionHits);
                return cacheItem.Value;
            }
            else
            {
                // 过期则移除
                ExpressionSqlCache.TryRemove(expressionKey, out _);
                Interlocked.Increment(ref _expirations);
            }
        }

        // 创建新缓存项
        var sql = factory(expressionKey);
        ExpressionSqlCache[expressionKey] = new CacheItem<string>(sql, DEFAULT_EXPIRATION);
        return sql;
    }

    // 获取或添加属性列名映射
    public static Dictionary<string, string> GetOrAddPropertyColumnMappings(Type type,
        Func<Type, Dictionary<string, string>> factory)
    {
        if (PropertyColumnMappingsCache.TryGetValue(type, out var cacheItem))
        {
            // 检查是否过期
            if (!cacheItem.IsExpired)
            {
                return cacheItem.Value;
            }
            else
            {
                // 过期则移除
                PropertyColumnMappingsCache.TryRemove(type, out _);
                Interlocked.Increment(ref _expirations);
            }
        }

        // 创建新缓存项
        var mappings = factory(type);
        PropertyColumnMappingsCache[type] = new CacheItem<Dictionary<string, string>>(mappings, DEFAULT_EXPIRATION);
        return mappings;
    }

    // 获取缓存统计信息
    public static string GetStatistics()
    {
        return $"属性缓存: {TypePropertiesCache.Count}项(命中:{_propertyHits}), " +
               $"列名缓存: {EntityColumnsCache.Count}项(命中:{_columnHits}), " +
               $"表达式缓存: {ExpressionSqlCache.Count}项(命中:{_expressionHits}), " +
               $"过期项: {_expirations}";
    }

    // 手动清理所有过期缓存
    public static void CleanupExpiredItems()
    {
        int cleaned = 0;

        // 清理属性缓存
        foreach (var key in TypePropertiesCache.Keys)
        {
            if (TypePropertiesCache.TryGetValue(key, out var item) && item.IsExpired)
            {
                TypePropertiesCache.TryRemove(key, out _);
                cleaned++;
            }
        }

        // 清理列名缓存
        foreach (var key in EntityColumnsCache.Keys)
        {
            if (EntityColumnsCache.TryGetValue(key, out var item) && item.IsExpired)
            {
                EntityColumnsCache.TryRemove(key, out _);
                cleaned++;
            }
        }

        // 清理属性列名映射缓存
        foreach (var key in PropertyColumnMappingsCache.Keys)
        {
            if (PropertyColumnMappingsCache.TryGetValue(key, out var item) && item.IsExpired)
            {
                PropertyColumnMappingsCache.TryRemove(key, out _);
                cleaned++;
            }
        }

        // 清理表达式缓存
        foreach (var key in ExpressionSqlCache.Keys)
        {
            if (ExpressionSqlCache.TryGetValue(key, out var item) && item.IsExpired)
            {
                ExpressionSqlCache.TryRemove(key, out _);
                cleaned++;
            }
        }

        // 更新过期计数
        Interlocked.Add(ref _expirations, cleaned);
    }
}
