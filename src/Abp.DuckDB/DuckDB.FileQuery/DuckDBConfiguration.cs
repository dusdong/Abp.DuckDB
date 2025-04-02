namespace Abp.DuckDB.FileQuery;

/// <summary>
/// DuckDB配置类，管理DuckDB查询提供程序的所有配置选项
/// </summary>
public class DuckDBConfiguration
{
    #region 连接配置
        
    /// <summary>
    /// 连接字符串
    /// </summary>
    public string ConnectionString { get; set; } = "Data Source=:memory:";
        
    /// <summary>
    /// 线程数
    /// </summary>
    public int ThreadCount { get; set; } = Math.Max(2, Math.Min(Environment.ProcessorCount - 1, 8));
        
    /// <summary>
    /// 内存限制
    /// </summary>
    public string MemoryLimit { get; set; } = "4GB";
        
    /// <summary>
    /// 是否启用压缩
    /// </summary>
    public bool EnableCompression { get; set; } = false;
        
    /// <summary>
    /// 压缩类型
    /// </summary>
    public string CompressionType { get; set; } = "UNCOMPRESSED";
        
    #endregion
        
    #region 缓存配置
        
    /// <summary>
    /// 最大缓存项数量
    /// </summary>
    public int MaxCacheSize { get; set; } = 1000;
        
    /// <summary>
    /// 缓存过期时间
    /// </summary>
    public TimeSpan CacheExpirationTime { get; set; } = TimeSpan.FromHours(12);
        
    /// <summary>
    /// 是否启用缓存
    /// </summary>
    public bool EnableCache { get; set; } = true;
        
    /// <summary>
    /// 缓存清理间隔
    /// </summary>
    public TimeSpan CacheCleanupInterval { get; set; } = TimeSpan.FromHours(1);
        
    #endregion
        
    #region 查询配置
        
    /// <summary>
    /// 默认是否使用流式模式
    /// </summary>
    public bool UseStreamingModeByDefault { get; set; } = true;
        
    /// <summary>
    /// 默认批处理大小
    /// </summary>
    public int DefaultBatchSize { get; set; } = 1000;
        
    /// <summary>
    /// 最大批处理大小
    /// </summary>
    public int MaxBatchSize { get; set; } = 10000;
        
    /// <summary>
    /// 命令超时时间
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromMinutes(5);
        
    /// <summary>
    /// 是否自动重试失败的查询
    /// </summary>
    public bool AutoRetryFailedQueries { get; set; } = true;
        
    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;
        
    /// <summary>
    /// 重试间隔
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(1);
        
    #endregion
        
    #region 监控配置
        
    /// <summary>
    /// 是否启用性能监控
    /// </summary>
    public bool EnablePerformanceMonitoring { get; set; } = true;
        
    /// <summary>
    /// 是否记录查询计划
    /// </summary>
    public bool LogQueryPlans { get; set; } = false;
        
    /// <summary>
    /// 是否收集详细指标
    /// </summary>
    public bool CollectDetailedMetrics { get; set; } = false;
        
    /// <summary>
    /// 是否记录慢查询
    /// </summary>
    public bool LogSlowQueries { get; set; } = true;
        
    /// <summary>
    /// 慢查询阈值（毫秒）
    /// </summary>
    public long SlowQueryThresholdMs { get; set; } = 1000;
        
    #endregion
        
    #region 高级配置
        
    /// <summary>
    /// 是否优化直接文件访问
    /// </summary>
    public bool OptimizeDirectFileAccess { get; set; } = true;
        
    /// <summary>
    /// 是否启用并行查询处理
    /// </summary>
    public bool EnableParallelQueryProcessing { get; set; } = true;
        
    /// <summary>
    /// 查询优化级别（0-3）
    /// </summary>
    public int OptimizationLevel { get; set; } = 2;
        
    #endregion
        
    /// <summary>
    /// 创建默认配置
    /// </summary>
    public static DuckDBConfiguration Default => new DuckDBConfiguration();
        
    /// <summary>
    /// 创建高性能配置
    /// </summary>
    public static DuckDBConfiguration HighPerformance()
    {
        return new DuckDBConfiguration
        {
            ThreadCount = Environment.ProcessorCount,
            MemoryLimit = "8GB",
            EnableCompression = true,
            CompressionType = "ZSTD",
            MaxCacheSize = 5000,
            UseStreamingModeByDefault = false,
            DefaultBatchSize = 5000,
            MaxBatchSize = 50000,
            EnableParallelQueryProcessing = true,
            OptimizationLevel = 3
        };
    }
        
    /// <summary>
    /// 创建低内存使用配置
    /// </summary>
    public static DuckDBConfiguration LowMemory()
    {
        return new DuckDBConfiguration
        {
            ThreadCount = Math.Max(2, Environment.ProcessorCount / 4),
            MemoryLimit = "1GB",
            EnableCompression = true,
            CompressionType = "ZSTD",
            MaxCacheSize = 500,
            UseStreamingModeByDefault = true,
            DefaultBatchSize = 500,
            MaxBatchSize = 2000,
            EnableParallelQueryProcessing = false,
            OptimizationLevel = 1
        };
    }
}
