using System.Collections.Concurrent;
using System.Reflection;

namespace Abp.DuckDB;

/// <summary>
/// DuckDB元数据缓存管理器，使用适应性淘汰策略
/// </summary>
public static class DuckDBMetadataCache
{
    #region 缓存存储

    // 类型-属性缓存
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _propertiesCache = new();

    // 类型-列映射缓存
    private static readonly ConcurrentDictionary<Type, Dictionary<string, string>> _propertyColumnMappingsCache = new();

    // 类型-实体列名缓存
    private static readonly ConcurrentDictionary<Type, List<string>> _entityColumnsCache = new();

    // 表达式字符串-SQL缓存
    private static readonly ConcurrentDictionary<string, string> _expressionSqlCache = new();

    // 属性访问频率统计
    private static readonly ConcurrentDictionary<Type, CacheMetrics> _accessMetrics = new();

    // 表达式访问频率统计
    private static readonly ConcurrentDictionary<string, ExpressionCacheMetrics> _expressionAccessMetrics = new();

    // 缓存配置
    private static DuckDBCacheConfiguration _config = new();

    // 清理计时器
    private static Timer _cleanupTimer;

    // 锁对象，用于安全初始化
    private static readonly object _lockObject = new object();

    // 上次清理时间
    private static DateTime _lastCleanupTime = DateTime.UtcNow;

    #endregion

    #region 公共方法

    /// <summary>
    /// 应用缓存配置
    /// </summary>
    public static void ApplyConfiguration(DuckDBConfiguration configuration)
    {
        if (configuration == null) return;

        lock (_lockObject)
        {
            _config.MaxEntries = configuration.MaxCacheEntries;
            _config.EnableAdaptiveEviction = configuration.EnableAdaptiveEviction;
            _config.EvictionCheckInterval = TimeSpan.FromMinutes(configuration.CacheEvictionIntervalMinutes);

            // 初始化或重置清理计时器
            if (_cleanupTimer == null)
            {
                _cleanupTimer = new Timer(
                    CleanupCache,
                    null,
                    _config.EvictionCheckInterval,
                    _config.EvictionCheckInterval);
            }
            else
            {
                _cleanupTimer.Change(_config.EvictionCheckInterval, _config.EvictionCheckInterval);
            }
        }
    }

    /// <summary>
    /// 获取或添加属性缓存
    /// </summary>
    public static PropertyInfo[] GetOrAddProperties(Type type, Func<Type, PropertyInfo[]> factory)
    {
        var result = _propertiesCache.GetOrAdd(type, factory);
        RecordTypeAccess(type);
        return result;
    }

    /// <summary>
    /// 获取或添加属性列映射缓存
    /// </summary>
    public static Dictionary<string, string> GetOrAddPropertyColumnMappings(Type type, Func<Type, Dictionary<string, string>> factory)
    {
        var result = _propertyColumnMappingsCache.GetOrAdd(type, factory);
        RecordTypeAccess(type);
        return result;
    }

    /// <summary>
    /// 获取或添加实体列名缓存
    /// </summary>
    public static List<string> GetOrAddEntityColumns(Type type, Func<Type, List<string>> factory)
    {
        var result = _entityColumnsCache.GetOrAdd(type, factory);
        RecordTypeAccess(type);
        return result;
    }

    /// <summary>
    /// 获取或添加表达式SQL缓存
    /// </summary>
    public static string GetOrAddExpressionSql(string expressionKey, Func<string, string> factory)
    {
        var result = _expressionSqlCache.GetOrAdd(expressionKey, factory);
        RecordExpressionAccess(expressionKey);
        return result;
    }

    /// <summary>
    /// 预热实体元数据
    /// </summary>
    public static void PrewarmEntityMetadata(Type entityType)
    {
        try
        {
            // 确保属性已缓存
            if (!_propertiesCache.ContainsKey(entityType))
            {
                var properties = entityType.GetProperties()
                    .Where(p => !p.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute), true).Any())
                    .ToArray();
                _propertiesCache[entityType] = properties;

                // 记录初始访问，提高优先级
                RecordTypeAccess(entityType);
                RecordTypeAccess(entityType); // 多记录一次增加热度
            }

            // 确保列映射已缓存
            if (!_propertyColumnMappingsCache.ContainsKey(entityType))
            {
                var mappings = new Dictionary<string, string>();
                foreach (var prop in _propertiesCache[entityType])
                {
                    var columnAttribute = prop.GetCustomAttributes(typeof(ColumnAttribute), true)
                        .FirstOrDefault() as ColumnAttribute;
                    mappings[prop.Name] = columnAttribute?.Name ?? prop.Name;
                }

                _propertyColumnMappingsCache[entityType] = mappings;
            }

            // 确保列名已缓存
            if (!_entityColumnsCache.ContainsKey(entityType))
            {
                var columns = _propertiesCache[entityType]
                    .Select(p =>
                    {
                        var columnAttribute = p.GetCustomAttributes(typeof(ColumnAttribute), true)
                            .FirstOrDefault() as ColumnAttribute;
                        return columnAttribute?.Name ?? p.Name;
                    })
                    .ToList();
                _entityColumnsCache[entityType] = columns;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"预热实体元数据失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 获取缓存统计信息
    /// </summary>
    public static string GetStatistics()
    {
        int totalMetadataItems = _propertiesCache.Count + _propertyColumnMappingsCache.Count + _entityColumnsCache.Count;
        int expressionItems = _expressionSqlCache.Count;
        int highFrequencyItems = _accessMetrics.Values.Count(m => m.AccessCount > 10);
        int lowFrequencyItems = _accessMetrics.Values.Count(m => m.AccessCount <= 3);

        var mostAccessedType = _accessMetrics
            .OrderByDescending(x => x.Value.AccessCount)
            .FirstOrDefault();

        var mostAccessedExpression = _expressionAccessMetrics
            .OrderByDescending(x => x.Value.AccessCount)
            .FirstOrDefault();

        string mostAccessedTypeInfo = mostAccessedType.Key != null ? $"最常访问类型: {mostAccessedType.Key.Name}({mostAccessedType.Value.AccessCount}次)" : "无类型访问记录";

        string mostAccessedExpressionInfo = mostAccessedExpression.Key != null ? $"最常用表达式访问次数: {mostAccessedExpression.Value.AccessCount}" : "无表达式访问记录";

        return $"元数据缓存项: {totalMetadataItems}, " +
               $"表达式缓存项: {expressionItems}, " +
               $"属性缓存: {_propertiesCache.Count}, " +
               $"列映射缓存: {_propertyColumnMappingsCache.Count}, " +
               $"实体列名缓存: {_entityColumnsCache.Count}, " +
               $"高频项: {highFrequencyItems}, " +
               $"低频项: {lowFrequencyItems}, " +
               $"{mostAccessedTypeInfo}, " +
               $"{mostAccessedExpressionInfo}";
    }

    /// <summary>
    /// 手动清理缓存
    /// </summary>
    /// <param name="evictionPercentage">清除百分比(0-100)</param>
    public static void ManualCleanup(int evictionPercentage = 20)
    {
        if (evictionPercentage <= 0 || evictionPercentage > 100)
            evictionPercentage = 20;

        lock (_lockObject)
        {
            // 清理元数据缓存
            int totalMetadataEntries = _propertiesCache.Count + _propertyColumnMappingsCache.Count + _entityColumnsCache.Count;
            int metadataEntriesToRemove = (int)(totalMetadataEntries * evictionPercentage / 100.0);

            if (metadataEntriesToRemove > 0)
            {
                PerformMetadataCleanup(metadataEntriesToRemove);
            }

            // 清理表达式缓存
            int totalExpressionEntries = _expressionSqlCache.Count;
            int expressionEntriesToRemove = (int)(totalExpressionEntries * evictionPercentage / 100.0);

            if (expressionEntriesToRemove > 0)
            {
                PerformExpressionCleanup(expressionEntriesToRemove);
            }
        }
    }

    /// <summary>
    /// 清空所有缓存
    /// </summary>
    public static void ClearCache()
    {
        _propertiesCache.Clear();
        _propertyColumnMappingsCache.Clear();
        _entityColumnsCache.Clear();
        _expressionSqlCache.Clear();
        _accessMetrics.Clear();
        _expressionAccessMetrics.Clear();
    }

    #endregion

    #region 内部方法

    /// <summary>
    /// 记录类型访问
    /// </summary>
    private static void RecordTypeAccess(Type type)
    {
        if (!_config.EnableAdaptiveEviction) return;

        _accessMetrics.AddOrUpdate(
            type,
            _ => new CacheMetrics { AccessCount = 1, LastAccessTime = DateTime.UtcNow },
            (_, metrics) =>
            {
                metrics.AccessCount++;
                metrics.LastAccessTime = DateTime.UtcNow;
                return metrics;
            });

        // 检查是否需要清理缓存（避免等待计时器而导致内存过高）
        CheckAndCleanupIfNeeded();
    }

    /// <summary>
    /// 记录表达式访问
    /// </summary>
    private static void RecordExpressionAccess(string expressionKey)
    {
        if (!_config.EnableAdaptiveEviction) return;

        _expressionAccessMetrics.AddOrUpdate(
            expressionKey,
            _ => new ExpressionCacheMetrics { AccessCount = 1, LastAccessTime = DateTime.UtcNow },
            (_, metrics) =>
            {
                metrics.AccessCount++;
                metrics.LastAccessTime = DateTime.UtcNow;
                return metrics;
            });

        // 检查是否需要清理表达式缓存
        CheckAndCleanupIfNeeded();
    }

    /// <summary>
    /// 检查并在必要时清理缓存
    /// </summary>
    private static void CheckAndCleanupIfNeeded()
    {
        // 检查清理间隔
        if ((DateTime.UtcNow - _lastCleanupTime) < TimeSpan.FromSeconds(30))
            return;

        int totalMetadataEntries = _propertiesCache.Count + _propertyColumnMappingsCache.Count + _entityColumnsCache.Count;
        int totalExpressionEntries = _expressionSqlCache.Count;

        // 如果任一缓存项超过最大数量的80%，则执行清理
        if (totalMetadataEntries > _config.MaxEntries * 0.8 ||
            totalExpressionEntries > _config.MaxEntries * 0.8)
        {
            lock (_lockObject)
            {
                // 双重检查
                if ((DateTime.UtcNow - _lastCleanupTime) < TimeSpan.FromSeconds(30))
                    return;

                // 清理元数据缓存
                if (totalMetadataEntries > _config.MaxEntries * 0.8)
                {
                    int entriesToRemove = (int)(totalMetadataEntries * 0.2); // 清理20%
                    PerformMetadataCleanup(entriesToRemove);
                }

                // 清理表达式缓存
                if (totalExpressionEntries > _config.MaxEntries * 0.8)
                {
                    int entriesToRemove = (int)(totalExpressionEntries * 0.2); // 清理20%
                    PerformExpressionCleanup(entriesToRemove);
                }

                _lastCleanupTime = DateTime.UtcNow;
            }
        }
    }

    /// <summary>
    /// 缓存清理方法
    /// </summary>
    private static void CleanupCache(object state)
    {
        if (!_config.EnableAdaptiveEviction) return;

        try
        {
            // 检查元数据缓存是否需要清理
            int totalMetadataEntries = _propertiesCache.Count + _propertyColumnMappingsCache.Count + _entityColumnsCache.Count;

            if (totalMetadataEntries > _config.MaxEntries)
            {
                int entriesToRemove = (int)(totalMetadataEntries * 0.2); // 清理20%
                PerformMetadataCleanup(entriesToRemove);
            }

            // 检查表达式缓存是否需要清理
            int totalExpressionEntries = _expressionSqlCache.Count;

            if (totalExpressionEntries > _config.MaxEntries)
            {
                int entriesToRemove = (int)(totalExpressionEntries * 0.2); // 清理20%
                PerformExpressionCleanup(entriesToRemove);
            }

            _lastCleanupTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            // 记录清理异常，但不抛出
            Console.Error.WriteLine($"缓存清理异常: {ex.Message}");
        }
    }

    /// <summary>
    /// 执行元数据缓存清理
    /// </summary>
    private static void PerformMetadataCleanup(int entriesToRemove)
    {
        if (entriesToRemove <= 0) return;

        try
        {
            // 获取访问指标并按评分排序（低评分先淘汰）
            var metrics = _accessMetrics.ToArray()
                .Select(kvp => new { Type = kvp.Key, Metrics = kvp.Value })
                .OrderBy(x => x.Metrics.CalculateScore())
                .Take(entriesToRemove)
                .ToList();

            int removedCount = 0;

            // 执行淘汰
            foreach (var item in metrics)
            {
                var type = item.Type;
                bool removedAny = false;

                if (_propertiesCache.TryRemove(type, out _))
                {
                    removedAny = true;
                    removedCount++;
                }

                if (_propertyColumnMappingsCache.TryRemove(type, out _))
                {
                    removedAny = true;
                    removedCount++;
                }

                if (_entityColumnsCache.TryRemove(type, out _))
                {
                    removedAny = true;
                    removedCount++;
                }

                if (removedAny)
                {
                    _accessMetrics.TryRemove(type, out _);
                }
            }

            if (removedCount > 0)
            {
                Console.WriteLine($"[DuckDB元数据缓存] 已清理 {removedCount} 个低使用频率的元数据缓存项");
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"执行元数据缓存清理失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 执行表达式缓存清理
    /// </summary>
    private static void PerformExpressionCleanup(int entriesToRemove)
    {
        if (entriesToRemove <= 0) return;

        try
        {
            // 获取表达式访问指标并按评分排序（低评分先淘汰）
            var metrics = _expressionAccessMetrics.ToArray()
                .Select(kvp => new { ExpressionKey = kvp.Key, Metrics = kvp.Value })
                .OrderBy(x => x.Metrics.CalculateScore())
                .Take(entriesToRemove)
                .ToList();

            int removedCount = 0;

            // 执行淘汰
            foreach (var item in metrics)
            {
                var expressionKey = item.ExpressionKey;

                if (_expressionSqlCache.TryRemove(expressionKey, out _))
                {
                    _expressionAccessMetrics.TryRemove(expressionKey, out _);
                    removedCount++;
                }
            }

            if (removedCount > 0)
            {
                Console.WriteLine($"[DuckDB表达式缓存] 已清理 {removedCount} 个低使用频率的表达式缓存项");
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"执行表达式缓存清理失败: {ex.Message}");
        }
    }

    #endregion

    #region 内部类

    /// <summary>
    /// 元数据缓存指标
    /// </summary>
    private class CacheMetrics
    {
        public int AccessCount { get; set; }
        public DateTime LastAccessTime { get; set; }

        /// <summary>
        /// 计算条目评分 (值越高越应该保留)
        /// </summary>
        public double CalculateScore()
        {
            // 结合访问频率和时间因素
            double frequencyFactor = Math.Log10(AccessCount + 1); // 访问次数对数，避免单一高频项过度主导
            double recencyFactor = 1.0 / (DateTime.UtcNow - LastAccessTime).TotalHours + 1; // 最近访问权重

            return frequencyFactor * recencyFactor;
        }
    }

    /// <summary>
    /// 表达式缓存指标
    /// </summary>
    private class ExpressionCacheMetrics
    {
        public int AccessCount { get; set; }
        public DateTime LastAccessTime { get; set; }

        /// <summary>
        /// 计算条目评分 (值越高越应该保留)
        /// </summary>
        public double CalculateScore()
        {
            // 表达式缓存评分更注重访问频率
            double frequencyFactor = Math.Log10(AccessCount + 1) * 1.5; // 加权更高
            double recencyFactor = 1.0 / (DateTime.UtcNow - LastAccessTime).TotalHours + 1;

            return frequencyFactor * recencyFactor;
        }
    }

    /// <summary>
    /// 缓存配置
    /// </summary>
    private class DuckDBCacheConfiguration
    {
        public int MaxEntries { get; set; } = 500; // 默认最大缓存条目
        public bool EnableAdaptiveEviction { get; set; } = true; // 启用自适应淘汰
        public TimeSpan EvictionCheckInterval { get; set; } = TimeSpan.FromMinutes(10); // 淘汰检查间隔
    }

    #endregion
}
