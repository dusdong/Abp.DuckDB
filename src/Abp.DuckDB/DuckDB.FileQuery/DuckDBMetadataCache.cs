using System.Collections.Concurrent;
using System.Reflection;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB 元数据缓存，用于优化查询性能
/// </summary>
public static class DuckDBMetadataCache
{
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _propertiesCache = new();
    private static readonly ConcurrentDictionary<Type, Dictionary<string, string>> _propertyColumnMappingsCache = new();
    private static readonly ConcurrentDictionary<Type, List<string>> _entityColumnsCache = new();
    private static readonly ConcurrentDictionary<string, CacheItem<string>> _expressionSqlCache = new();

    private static Timer _cleanupTimer;
    private static TimeSpan _defaultExpirationTime = TimeSpan.FromHours(12);
    private static int _maxCacheSize = 1000;
    private static readonly object _lockObject = new();
    private static bool _cleanupRunning = false;
    private static bool _cacheEnabled = true;

    // 缓存统计
    private static int _expressionCacheHits = 0;
    private static int _expressionCacheMisses = 0;
    private static int _evictionCount = 0;
    private static int _cleanupCount = 0;

    /// <summary>
    /// 应用配置到元数据缓存
    /// </summary>
    public static void ApplyConfiguration(DuckDBConfiguration configuration)
    {
        if (configuration == null) return;
            
        _maxCacheSize = configuration.MaxCacheSize;
        _defaultExpirationTime = configuration.CacheExpirationTime;
        _cacheEnabled = configuration.EnableCache;
            
        // 重新配置清理定时器
        lock (_lockObject)
        {
            _cleanupTimer?.Dispose();
            _cleanupTimer = new Timer(CleanupCallback, null, 
                configuration.CacheCleanupInterval, 
                configuration.CacheCleanupInterval);
        }
    }

    static DuckDBMetadataCache()
    {
        // 创建定时清理任务，每小时运行一次
        _cleanupTimer = new Timer(CleanupCallback, null, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
    }

    /// <summary>
    /// 获取或添加属性缓存
    /// </summary>
    public static PropertyInfo[] GetOrAddProperties(Type type, Func<Type, PropertyInfo[]> valueFactory)
    {
        if (!_cacheEnabled)
            return valueFactory(type);
                
        return _propertiesCache.GetOrAdd(type, valueFactory);
    }

    /// <summary>
    /// 获取或添加属性列映射缓存
    /// </summary>
    public static Dictionary<string, string> GetOrAddPropertyColumnMappings(Type type, Func<Type, Dictionary<string, string>> valueFactory)
    {
        if (!_cacheEnabled)
            return valueFactory(type);
                
        return _propertyColumnMappingsCache.GetOrAdd(type, valueFactory);
    }

    /// <summary>
    /// 获取或添加实体列缓存
    /// </summary>
    public static List<string> GetOrAddEntityColumns(Type type, Func<Type, List<string>> valueFactory)
    {
        if (!_cacheEnabled)
            return valueFactory(type);
                
        return _entityColumnsCache.GetOrAdd(type, valueFactory);
    }

    /// <summary>
    /// 获取或添加表达式SQL缓存，带过期时间
    /// </summary>
    public static string GetOrAddExpressionSql(string expressionKey, Func<string, string> valueFactory)
    {
        if (!_cacheEnabled)
            return valueFactory(expressionKey);
                
        CheckCacheSize();

        if (_expressionSqlCache.TryGetValue(expressionKey, out var cacheItem) && !cacheItem.IsExpired)
        {
            // 缓存命中
            Interlocked.Increment(ref _expressionCacheHits);
            cacheItem.LastAccessed = DateTime.UtcNow; // 更新访问时间
            return cacheItem.Value;
        }

        // 缓存未命中
        Interlocked.Increment(ref _expressionCacheMisses);
        var value = valueFactory(expressionKey);
        _expressionSqlCache[expressionKey] = new CacheItem<string>(value, DateTime.UtcNow.Add(_defaultExpirationTime));
        return value;
    }

    /// <summary>
    /// 设置自定义过期时间的表达式SQL缓存
    /// </summary>
    public static string GetOrAddExpressionSql(string expressionKey, Func<string, string> valueFactory, TimeSpan expirationTime)
    {
        if (!_cacheEnabled)
            return valueFactory(expressionKey);
                
        CheckCacheSize();

        if (_expressionSqlCache.TryGetValue(expressionKey, out var cacheItem) && !cacheItem.IsExpired)
        {
            Interlocked.Increment(ref _expressionCacheHits);
            cacheItem.LastAccessed = DateTime.UtcNow;
            return cacheItem.Value;
        }

        Interlocked.Increment(ref _expressionCacheMisses);
        var value = valueFactory(expressionKey);
        _expressionSqlCache[expressionKey] = new CacheItem<string>(value, DateTime.UtcNow.Add(expirationTime));
        return value;
    }

    /// <summary>
    /// 检查缓存大小并清理过期项
    /// </summary>
    private static void CheckCacheSize()
    {
        // 如果表达式缓存超过最大大小，则清理最老的20%
        if (_expressionSqlCache.Count > _maxCacheSize && !_cleanupRunning)
        {
            Task.Run(() => TrimExpressionCache());
        }
    }

    /// <summary>
    /// 清理最不常用的缓存项
    /// </summary>
    private static void TrimExpressionCache()
    {
        if (_cleanupRunning) return;

        lock (_lockObject)
        {
            if (_cleanupRunning) return;
            _cleanupRunning = true;

            try
            {
                // 移除所有过期项
                int removed = RemoveExpiredItems();
                Interlocked.Add(ref _evictionCount, removed);

                if (_expressionSqlCache.Count > _maxCacheSize)
                {
                    // 按最后访问时间排序并移除最老的20%
                    var itemsToRemove = _expressionSqlCache
                        .OrderBy(kv => kv.Value.LastAccessed)
                        .ThenBy(kv => kv.Value.AccessCount)
                        .Take(_expressionSqlCache.Count / 5)
                        .Select(kv => kv.Key)
                        .ToList();

                    foreach (var key in itemsToRemove)
                    {
                        _expressionSqlCache.TryRemove(key, out _);
                    }
                        
                    Interlocked.Add(ref _evictionCount, itemsToRemove.Count);
                }
                    
                Interlocked.Increment(ref _cleanupCount);
            }
            finally
            {
                _cleanupRunning = false;
            }
        }
    }

    /// <summary>
    /// 定时清理过期缓存项
    /// </summary>
    private static void CleanupCallback(object state)
    {
        if (!_cacheEnabled) return;
            
        int removed = RemoveExpiredItems();
        if (removed > 0)
        {
            Interlocked.Add(ref _evictionCount, removed);
            Interlocked.Increment(ref _cleanupCount);
        }
    }

    /// <summary>
    /// 移除所有过期的缓存项
    /// </summary>
    private static int RemoveExpiredItems()
    {
        var expiredKeys = _expressionSqlCache
            .Where(kv => kv.Value.IsExpired)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var key in expiredKeys)
        {
            _expressionSqlCache.TryRemove(key, out _);
        }
            
        return expiredKeys.Count;
    }

    /// <summary>
    /// 获取缓存统计信息
    /// </summary>
    public static string GetStatistics()
    {
        var hitRate = _expressionCacheHits + _expressionCacheMisses > 0
            ? (double)_expressionCacheHits / (_expressionCacheHits + _expressionCacheMisses) * 100
            : 0;

        return $"属性缓存数: {_propertiesCache.Count}, " +
               $"列映射缓存数: {_propertyColumnMappingsCache.Count}, " +
               $"实体列缓存数: {_entityColumnsCache.Count}, " +
               $"表达式缓存数: {_expressionSqlCache.Count}, " +
               $"表达式缓存命中率: {hitRate:F2}%, " +
               $"缓存淘汰数: {_evictionCount}, " +
               $"清理次数: {_cleanupCount}";
    }
        
    /// <summary>
    /// 获取详细的缓存统计信息
    /// </summary>
    public static CacheStatistics GetDetailedStatistics()
    {
        return new CacheStatistics
        {
            PropertyCacheCount = _propertiesCache.Count,
            ColumnMappingCacheCount = _propertyColumnMappingsCache.Count,
            EntityColumnCacheCount = _entityColumnsCache.Count,
            ExpressionSqlCacheCount = _expressionSqlCache.Count,
            ExpressionCacheHits = _expressionCacheHits,
            ExpressionCacheMisses = _expressionCacheMisses,
            EvictionCount = _evictionCount,
            CleanupCount = _cleanupCount,
            CacheEnabled = _cacheEnabled,
            MaxCacheSize = _maxCacheSize,
            ExpirationTimeHours = _defaultExpirationTime.TotalHours,
            CacheHitRate = _expressionCacheHits + _expressionCacheMisses > 0
                ? (double)_expressionCacheHits / (_expressionCacheHits + _expressionCacheMisses) * 100
                : 0
        };
    }

    /// <summary>
    /// 清除表达式缓存
    /// </summary>
    public static void ClearExpressionCache()
    {
        _expressionSqlCache.Clear();
        ResetStatistics();
    }

    /// <summary>
    /// 清除所有缓存
    /// </summary>
    public static void ClearAllCaches()
    {
        _propertiesCache.Clear();
        _propertyColumnMappingsCache.Clear();
        _entityColumnsCache.Clear();
        _expressionSqlCache.Clear();
        ResetStatistics();
    }

    /// <summary>
    /// 重置统计数据
    /// </summary>
    public static void ResetStatistics()
    {
        Interlocked.Exchange(ref _expressionCacheHits, 0);
        Interlocked.Exchange(ref _expressionCacheMisses, 0);
        Interlocked.Exchange(ref _evictionCount, 0);
        Interlocked.Exchange(ref _cleanupCount, 0);
    }

    /// <summary>
    /// 缓存项包装类，包含过期时间和最后访问时间
    /// </summary>
    private class CacheItem<T>
    {
        public T Value { get; }
        public DateTime Expiration { get; }
        public DateTime LastAccessed { get; set; }
        public long AccessCount { get; set; }

        public bool IsExpired => DateTime.UtcNow > Expiration;

        public CacheItem(T value, DateTime expiration)
        {
            Value = value;
            Expiration = expiration;
            LastAccessed = DateTime.UtcNow;
            AccessCount = 1;
        }
    }
        
    /// <summary>
    /// 缓存统计信息类
    /// </summary>
    public class CacheStatistics
    {
        public int PropertyCacheCount { get; set; }
        public int ColumnMappingCacheCount { get; set; }
        public int EntityColumnCacheCount { get; set; }
        public int ExpressionSqlCacheCount { get; set; }
        public int ExpressionCacheHits { get; set; }
        public int ExpressionCacheMisses { get; set; }
        public int EvictionCount { get; set; }
        public int CleanupCount { get; set; }
        public bool CacheEnabled { get; set; }
        public int MaxCacheSize { get; set; }
        public double ExpirationTimeHours { get; set; }
        public double CacheHitRate { get; set; }
    }

    // 在应用程序关闭时停止定时器
    public static void Shutdown()
    {
        _cleanupTimer?.Dispose();
    }
}
