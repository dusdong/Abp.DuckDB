# DuckDBFileQueryProvider 编程指南与最佳实践

## 目录

1. [概述](#概述)
2. [初始化与配置](#初始化与配置)
3. [基本查询操作](#基本查询操作)
4. [流式数据处理](#流式数据处理)
5. [聚合查询](#聚合查询)
6. [分组聚合](#分组聚合)
7. [元组映射优化](#元组映射优化)
8. [性能优化策略](#性能优化策略)
9. [异常处理](#异常处理)
10. [最佳实践](#最佳实践)
11. [常见问题与解决方案](#常见问题与解决方案)

## 概述

`DuckDBFileQueryProvider` 类是一个专用于高效查询和处理 Parquet 文件的组件，它继承自 `DuckDbProviderBase` 并提供了丰富的 API 来直接查询、聚合和流式处理 Parquet 文件中的数据，无需将数据导入到关系数据库中。该类针对大数据场景进行了优化，特别适合对时序数据、日志文件和大型数据集的分析处理。

### 主要功能

- 直接查询 Parquet 文件，支持复杂过滤条件
- 优化的流式处理大型数据集，避免内存溢出
- 丰富的聚合函数（SUM, AVG, MIN, MAX, COUNT）
- 支持分组聚合和自定义聚合查询
- 高效的元组映射机制，支持复杂结果集映射
- 性能监控和诊断能力

## 初始化与配置

### 基本初始化

```csharp
// 通过依赖注入获取实例
var provider = IocManager.Resolve<IDuckDBFileQueryProvider>();

// 初始化配置
var config = new DuckDBConfiguration { 
    ConnectionString = "Data Source=:memory:",
    ThreadCount = 4,
    MemoryLimit = "8GB",
    EnablePerformanceMonitoring = true
};
provider.Initialize(config);
```

### 高级配置

```csharp
// 针对大数据处理的高级配置
var config = new DuckDBConfiguration {
    ConnectionString = "Data Source=:memory:",
    ThreadCount = Environment.ProcessorCount, // 使用所有可用核心
    MemoryLimit = "16GB",               // 适当增加内存限制
    MaxBatchSize = 10000,              // 批处理大小
    EnableCompression = true,          // 启用压缩
    CompressionType = "ZSTD",          // 使用ZSTD压缩算法
    OptimizationLevel = 3,             // 最高优化级别
    EnableCache = true,                // 启用查询缓存
    EnablePerformanceMonitoring = true // 启用性能监控
};
provider.Initialize(config);

// 应用优化设置
await provider.ApplyOptimizationAsync();
```

## 基本查询操作

### 查询单个 Parquet 文件

```csharp
// 查询单个 Parquet 文件
var results = await provider.QueryParquetFileAsync<LogEntry>(
    "path/to/logs.parquet",
    log => log.Level == "ERROR" && log.Timestamp > DateTime.Today.AddDays(-7)
);
```

### 查询多个 Parquet 文件

```csharp
// 查询多个文件 - 例如分区数据
var filePaths = new[] {
    "data/2023/01/01.parquet",
    "data/2023/01/02.parquet",
    "data/2023/01/03.parquet"
};

var results = await provider.QueryAsync<SalesRecord>(
    filePaths,
    record => record.Region == "East" && record.Amount > 1000
);
```

### 获取单条记录

```csharp
// 获取首条匹配记录
var firstMatch = await provider.FirstOrDefaultAsync<LogEntry>(
    filePaths,
    log => log.UserId == 123 && log.EventType == "Login"
);

if (firstMatch != null)
{
    Console.WriteLine($"找到匹配记录: {firstMatch.Timestamp}");
}
```

### 分页查询

```csharp
// 分页查询大型数据集
var pageResult = await provider.QueryPagedAsync<ProductData>(
    filePaths,
    product => product.Category == "Electronics",
    pageIndex: 0,     // 第一页
    pageSize: 50,     // 每页50条
    orderBy: p => p.Price,  // 按价格排序
    ascending: false   // 降序
);

Console.WriteLine($"总记录数: {pageResult.TotalCount}");
foreach (var item in pageResult.Items)
{
    Console.WriteLine($"{item.Name}: ¥{item.Price}");
}
```

## 流式数据处理

### 批处理流式查询

```csharp
// 批量处理大型数据集
int processedCount = await provider.QueryStreamAsync<SensorReading>(
    largeFilePaths,
    reading => reading.SensorType == "Temperature" && reading.Value > 30,
    batchSize: 5000,   // 每批5000条记录
    async batch => {
        // 处理每一批数据
        await ProcessTemperatureReadingsAsync(batch);
        
        // 可以在这里做聚合、过滤或保存等操作
    }
);

Console.WriteLine($"总共处理了 {processedCount} 条记录");
```

### 异步枚举器流式处理

```csharp
// 使用异步枚举器逐条处理 - 适合内存受限场景
await foreach (var reading in provider.QueryStreamEnumerableAsync<SensorReading>(
    largeFilePaths,
    reading => reading.Value > threshold,
    batchSize: 1000))  // 内部缓冲区大小
{
    // 逐条处理每条记录
    await ProcessReadingAsync(reading);
    
    // 计数或其他跟踪
    processedCount++;
    
    if (processedCount % 10000 == 0)
        Console.WriteLine($"已处理 {processedCount:N0} 条记录");
}
```

## 聚合查询

### 基本聚合函数

```csharp
// 计数查询
int totalErrors = await provider.CountAsync<LogEntry, int>(
    logFiles,
    log => log.Level == "ERROR"
);

// 求和查询
decimal totalSales = await provider.SumAsync<SalesRecord, decimal>(
    salesFiles,
    sale => sale.Amount,
    sale => sale.Date >= startDate && sale.Date <= endDate
);

// 平均值查询
double avgTemperature = await provider.AvgAsync<SensorReading, double>(
    sensorFiles,
    reading => reading.Value,
    reading => reading.SensorId == targetSensor
);

// 最大值查询
DateTime lastActivity = await provider.MaxAsync<UserActivity, DateTime>(
    activityFiles,
    activity => activity.Timestamp,
    activity => activity.UserId == userId
);

// 最小值查询
decimal lowestStock = await provider.MinAsync<InventoryRecord, decimal>(
    inventoryFiles,
    record => record.StockLevel,
    record => record.ProductId == productId
);
```

### 基于列名的聚合

```csharp
// 通过列名直接求和 - 适用于动态列名场景
decimal totalRevenue = await provider.SumAsync<SalesRecord, decimal>(
    salesFiles,
    "Revenue",  // 列名作为字符串
    sale => sale.Region == "North" && sale.Date.Year == 2023
);
```

## 分组聚合

### 分组计数

```csharp
// 按状态分组统计工单数量
var ticketsByStatus = await provider.CountByAsync<Ticket, string, int>(
    ticketFiles,
    ticket => ticket.Status,  // 分组键
    ticket => ticket.CreatedDate >= DateTime.Today.AddDays(-30)  // 过滤条件
);

// 输出结果
foreach (var (status, count) in ticketsByStatus)
{
    Console.WriteLine($"{status}: {count}");
}
```

### 分组求和

```csharp
// 按区域分组计算销售总额
var salesByRegion = await provider.SumByAsync<SalesRecord, string, decimal>(
    salesFiles,
    sale => sale.Amount,      // 求和字段
    sale => sale.Region,      // 分组键
    sale => sale.Date.Year == 2023,  // 过滤条件
    orderByColumn: "Sum",     // 按Sum列排序
    ascending: false          // 降序排列
);

// 输出结果
foreach (var (region, totalSales) in salesByRegion)
{
    Console.WriteLine($"{region}: {totalSales:C}");
}
```

### 自定义分组查询

```csharp
// 自定义复杂分组查询 - 使用元组返回多个聚合结果
var productAnalytics = await provider.ExecuteGroupByQueryAsync<(string Category, int Count, decimal AvgPrice, decimal TotalSales)>(
    productFiles,
    "Category, COUNT(*) AS Count, AVG(Price) AS AvgPrice, SUM(Price * Quantity) AS TotalSales",  // 选择子句
    "IsActive = true AND Price > 0",  // WHERE子句
    "Category",  // GROUP BY子句
    "TotalSales DESC"  // ORDER BY子句
);

// 输出结果
foreach (var (category, count, avgPrice, totalSales) in productAnalytics)
{
    Console.WriteLine($"{category}: {count} 产品, 平均价格: {avgPrice:C}, 总销售额: {totalSales:C}");
}
```

## 元组映射优化

### 确保列名与元组字段匹配

```csharp
// 确保SQL中的列别名与元组字段名匹配
var results = await provider.ExecuteGroupByQueryAsync<(int AccountId, decimal Sum)>(
    files,
    "AccountId AS AccountId, SUM(Amount) AS Sum",  // 明确指定列别名
    "TransactionDate BETWEEN '2023-01-01' AND '2023-12-31'",
    "AccountId",
    "Sum DESC"
);

// 记录实际结果以便调试
_logger.Debug($"查询结果: {string.Join(", ", results.Select(r => $"({r.AccountId}, {r.Sum})"))}");
```

### 自定义元组查询

```csharp
// 使用专门的元组查询方法处理ValueTuple结果
var sql = $@"
    SELECT Region AS Key, COUNT(*) AS Count, SUM(Revenue) AS Total 
    FROM read_parquet(['{string.Join("', '", filePaths)}'])
    WHERE Year = 2023
    GROUP BY Region
    ORDER BY Total DESC";

var regionStats = await provider.ExecuteTupleQueryAsync<(string Key, int Count, decimal Total)>(sql);
```

## 性能优化策略

### 文件路径优化

```csharp
// 对于大量文件，使用高效的路径构建
var filePattern = "logs/2023/*/events_*.parquet";
var matchingFiles = Directory.GetFiles("logs", "*.parquet", SearchOption.AllDirectories)
    .Where(f => f.Contains("events_") && f.Contains("/2023/"))
    .ToList();

// 添加日志记录文件数量以便诊断
_logger.Debug($"找到 {matchingFiles.Count} 个匹配文件");

var results = await provider.QueryAsync<LogEvent>(
    matchingFiles,
    e => e.Severity >= SeverityLevel.Error
);
```

### 查询优化

```csharp
// 先应用过滤以减少要处理的数据量
var results = await provider.QueryAsync<SalesRecord>(
    filePaths,
    r => r.Date >= startDate && r.Date <= endDate && r.Amount > 0  // 先应用日期过滤
);

// 不需要的列使用投影排除
var selectedColumns = new[] { "Id", "Date", "Amount", "CustomerId" };
var sql = $"SELECT {string.Join(", ", selectedColumns)} FROM read_parquet([...]) WHERE ...";
```

### 批处理大小调优

```csharp
// 根据记录大小调整批处理大小
int batchSize = CalculateOptimalBatchSize(
    estimatedRecordSizeInBytes: 1024, // 每条记录约1KB
    availableMemoryMB: 1000,          // 分配1GB内存给批处理
    overheadFactor: 1.2               // 考虑20%的内存开销
);

// 对大型记录使用较小的批量，对小型记录使用较大的批量
await provider.QueryStreamAsync<TEntity>(
    filePaths, 
    predicate,
    batchSize: batchSize, 
    ProcessBatchAsync
);
```

## 异常处理

```csharp
try
{
    var results = await provider.QueryAsync<LogEntry>(
        filePaths,
        log => log.Level == "ERROR"
    );
    // 处理结果...
}
catch (FileNotFoundException ex)
{
    _logger.Error($"找不到Parquet文件: {ex.FileName}");
    // 处理文件未找到情况
}
catch (DuckDBOperationException ex)
{
    _logger.Error($"DuckDB操作失败: {ex.Message}", ex);
    if (ex.Message.Contains("memory"))
    {
        // 处理内存不足情况
        _logger.Warn("尝试减小批处理大小或增加内存限制");
    }
}
catch (Exception ex)
{
    _logger.Error($"查询处理失败: {ex.Message}", ex);
    // 其他错误处理
}
```

## 最佳实践

### 1. 文件路径管理

- **使用绝对路径**：确保在不同环境中正确解析文件路径
- **预先验证文件存在性**：在批量操作前预检查文件是否存在，避免处理中断
- **使用文件分区策略**：按日期、区域等维度组织文件，便于查询特定数据子集

```csharp
// 良好的文件路径管理示例
var baseDir = Path.GetFullPath("data/sales");
var yearMonth = "2023-06";
var filePaths = Directory.GetFiles(Path.Combine(baseDir, yearMonth), "*.parquet")
    .Where(File.Exists)  // 额外验证文件存在
    .ToList();

if (filePaths.Count == 0)
{
    _logger.Warn($"未找到时间段 {yearMonth} 的数据文件");
    return new List<SaleRecord>();
}
```

### 2. 性能优化

- **先过滤后投影**：尽早应用过滤条件减少数据量
- **按需选择列**：只选择必要的列减少数据传输和处理量
- **合理设置批处理大小**：根据记录大小和可用内存调整批处理大小
- **使用适当的并行度**：配置与硬件匹配的线程数
- **监控并调整性能**：利用内置性能监控进行持续优化

```csharp
// 示例: 合理设置DuckDB配置
var config = new DuckDBConfiguration
{
    ThreadCount = Math.Max(1, Environment.ProcessorCount - 1),  // 留一个核心给系统
    MemoryLimit = Environment.Is64BitProcess ? "8GB" : "2GB",   // 根据进程位数调整
    EnablePerformanceMonitoring = true
};
```

### 3. 元组映射优化

- **明确指定列别名**：确保SQL中的列别名与元组字段名精确匹配
- **保持列顺序一致**：结果集中列的顺序应与元组字段顺序一致
- **使用显式类型转换**：对于可能的类型不匹配问题，使用SQL中的CAST
- **诊断列映射问题**：使用调试日志记录列名和类型信息

```csharp
// 良好的元组映射示例
var analytics = await provider.ExecuteGroupByQueryAsync<(string Region, int Count, decimal Total)>(
    filePaths,
    "Region AS Region, COUNT(*) AS Count, CAST(SUM(Revenue) AS DECIMAL(18,2)) AS Total",
    "IsValid = true",
    "Region",
    "Total DESC"
);
```

### 4. 错误处理与恢复

- **实现断点续传**：记录已处理状态，支持从中断点恢复
- **分批处理异常**：单条记录错误不应影响整批数据处理
- **详细日志记录**：记录足够的上下文信息以便诊断问题
- **优雅降级**：当遇到性能问题时，考虑降低批处理大小或限制查询范围

```csharp
// 断点续传示例
public async Task ProcessDataWithRecovery(IEnumerable<string> filePaths, string checkpointFile)
{
    // 尝试加载上次处理位置
    var lastProcessed = File.Exists(checkpointFile) 
        ? int.Parse(await File.ReadAllTextAsync(checkpointFile)) 
        : 0;
        
    int currentCount = lastProcessed;
    
    try
    {
        await provider.QueryStreamAsync<DataRecord>(
            filePaths,
            null,
            batchSize: 5000,
            async batch => 
            {
                await ProcessBatchAsync(batch);
                currentCount += batch.Count();
                
                // 每1万条记录保存一次检查点
                if (currentCount / 10000 > lastProcessed / 10000)
                {
                    await File.WriteAllTextAsync(checkpointFile, currentCount.ToString());
                }
            }
        );
    }
    catch (Exception ex)
    {
        _logger.Error($"处理中断: {ex.Message}，已处理 {currentCount} 条记录");
        throw;
    }
}
```

## 常见问题与解决方案

### 问题1: 内存使用过高

**症状**: 处理大型文件时出现内存不足异常或系统响应缓慢

**解决方案**:
- 减少批处理大小 (`batchSize` 参数)
- 增加流式处理而非一次性加载
- 使用更精确的过滤条件减少处理数据量
- 调整 DuckDB 内存限制 (`MemoryLimit` 配置)
- 对超大文件考虑预处理或分割

```csharp
// 内存优化示例
// 1. 使用流式处理
await provider.QueryStreamAsync<LargeRecord>(
    largeFiles,
    record => record.IsRelevant,
    batchSize: 1000,  // 减小批处理大小
    ProcessBatchAsync
);

// 2. 优化配置
provider.Initialize(new DuckDBConfiguration {
    MemoryLimit = "4GB",  // 限制内存使用
    DefaultBatchSize = 2000,
    EnableCompression = true  // 启用压缩减少内存占用
});
```

### 问题2: 元组映射返回默认值

**症状**: 查询结果中元组字段为默认值 (0, null 等)，尽管数据存在

**解决方案**:
- 确保SQL中的列别名与元组字段名匹配
- 使用 `AS` 关键字明确指定列别名
- 启用详细日志记录诊断映射问题
- 检查类型兼容性，必要时使用显式类型转换

```csharp
// 修复元组映射问题
// 1. 记录数据库列信息
_logger.Debug($"查询返回列: {string.Join(", ", GetColumnNamesFromReader(reader))}");

// 2. 确保列别名与元组字段匹配
var results = await provider.ExecuteGroupByQueryAsync<(int UserId, decimal TotalValue)>(
    files,
    "UserId AS UserId, SUM(Value) AS TotalValue",  // 精确匹配字段名
    null,
    "UserId"
);
```

### 问题3: 文件访问错误

**症状**: 出现 FileNotFoundException 或 UnauthorizedAccessException

**解决方案**:
- 使用绝对路径而非相对路径
- 确保应用有足够的文件访问权限
- 实现文件预验证逻辑
- 对于网络文件，考虑添加重试机制

```csharp
// 文件访问优化
public IEnumerable<string> GetAccessibleFiles(string[] candidatePaths)
{
    foreach (var path in candidatePaths)
    {
        try
        {
            // 验证文件存在且可读
            if (File.Exists(path))
            {
                using (File.OpenRead(path))
                {
                    // 文件可以打开，说明有权限
                }
                yield return path;
            }
        }
        catch (Exception ex) when (ex is FileNotFoundException || 
                                   ex is UnauthorizedAccessException ||
                                   ex is IOException)
        {
            _logger.Warn($"忽略无法访问的文件: {path}, 原因: {ex.Message}");
            // 跳过此文件继续处理
        }
    }
}

// 使用可访问的文件
var accessibleFiles = GetAccessibleFiles(candidatePaths).ToArray();
var results = await provider.QueryAsync<DataRecord>(accessibleFiles, ...);
```

### 问题4: 查询性能不佳

**症状**: 查询执行缓慢，尤其是对多个大型文件的查询

**解决方案**:
- 优化过滤条件，尽早应用过滤
- 增加线程数 (`ThreadCount` 配置)
- 使用索引优化查询
- 考虑预聚合或预处理大型数据集
- 监控并分析慢查询，调整查询策略

```csharp
// 性能优化
// 1. 应用配置优化
await provider.ApplyOptimizationAsync();

// 2. 获取性能报告分析瓶颈
var perfReport = provider.GetPerformanceReport();
_logger.Info($"查询性能: 平均 {perfReport.AverageQueryTimeMs}ms, " +
             $"吞吐量: {perfReport.RecordsPerSecond}/秒");

// 3. 优化查询逻辑
// 先进行粗粒度过滤找出候选记录
var candidates = await provider.QueryAsync<LogEntry>(
    logFiles,
    log => log.Date >= startDate && log.Date <= endDate  // 首先按日期过滤
);

// 然后在内存中进行细粒度过滤
var filteredResults = candidates
    .Where(log => ComplexFilterPredicate(log))
    .ToList();
```

### 问题5: 处理异常字符或编码问题

**症状**: 解析文件时出现字符编码问题或格式不一致错误

**解决方案**:
- 确保数据文件使用一致的编码
- 添加数据清洗和验证步骤
- 使用异常处理跳过有问题的记录
- 考虑自定义类型转换逻辑处理特殊情况

```csharp
// 处理编码问题
await provider.QueryStreamAsync<TextRecord>(
    textFiles,
    null,
    batchSize: 1000,
    async batch => {
        foreach (var record in batch)
        {
            try {
                // 尝试处理并清洗数据
                var cleanedText = CleanText(record.Content);
                await ProcessCleanedRecordAsync(record.Id, cleanedText);
            }
            catch (Exception ex) when (IsEncodingRelatedError(ex))
            {
                _logger.Warn($"跳过ID为 {record.Id} 的记录，编码问题: {ex.Message}");
                await LogProblematicRecordAsync(record.Id, ex);
            }
        }
    }
);

private bool IsEncodingRelatedError(Exception ex)
{
    return ex is DecoderFallbackException || 
           ex.Message.Contains("Invalid character") ||
           ex.Message.Contains("encoding");
}
```

通过遵循这些最佳实践和解决方案，您可以充分利用`DuckDBFileQueryProvider`类高效处理Parquet文件数据，实现高性能、低内存占用的大数据处理流程。