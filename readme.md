# Abp.DuckDB

[![NuGet](https://img.shields.io/nuget/v/Abp.DuckDB.svg?style=flat-square)](https://www.nuget.org/packages/Abp.DuckDB)
[![NuGet Download](https://img.shields.io/nuget/dt/Abp.DuckDB.svg?style=flat-square)](https://www.nuget.org/packages/Abp.DuckDB)
[![Build Status](https://img.shields.io/github/workflow/status/yourusername/Abp.DuckDB/build-and-test?style=flat-square)](https://github.com/yourusername/Abp.DuckDB/actions)
[![License](https://img.shields.io/github/license/yourusername/Abp.DuckDB?style=flat-square)](https://github.com/yourusername/Abp.DuckDB/blob/master/LICENSE)

Abp.DuckDB 是 [ABP 框架](https://abp.io) 的扩展模块，为 ABP 应用程序提供高性能、嵌入式的 [DuckDB](https://duckdb.org/) 数据库集成。DuckDB 是一款面向分析型工作负载的列式内存数据库系统，特别适合 OLAP 查询、数据分析和处理 Parquet 文件。

## 核心特性

- ABP 框架集成：完全兼容 ABP 生态系统
- 完整的 Repository 模式实现：与 ABP 规范一致的 CRUD 操作
- 多租户支持：内置租户数据隔离和管理
- Parquet 文件处理：原生支持高效读写 Parquet 格式文件
- 资源监控：租户资源使用跟踪和限制
- 高性能查询：针对分析型工作负载优化
- 事务管理：完整的事务支持
- 连接池：高效的连接管理

## 安装

```bash
dotnet add package Abp.DuckDB
```

## 基础配置

### 注册模块

在你的 ABP 模块中添加对 `AbpDuckDBModule` 的依赖：

```csharp
[DependsOn(
    // 其他依赖...
    typeof(AbpDuckDBModule)
)]
public class YourAppModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        // 配置 DuckDB
        Configure<AbpDuckDBOptions>(options =>
        {
            options.DatabaseDirectory = "App_Data/Databases";
            options.EnableMultiTenancy = true;
        });
    }
}
```

### 基本使用

```csharp
public class ProductService : ITransientDependency
{
    private readonly IDuckDBRepository<Product> _productRepository;
    
    public ProductService(IDuckDBRepository<Product> productRepository)
    {
        _productRepository = productRepository;
    }
    
    public async Task<List<Product>> GetProductsAsync()
    {
        return await _productRepository.QueryAsync();
    }
    
    public async Task CreateProductAsync(Product product)
    {
        await _productRepository.InsertAsync(product);
    }
}
```

## 多租户支持

Abp.DuckDB 提供了完整的多租户支持，每个租户可以拥有独立的数据库：

### 配置多租户

```csharp
Configure<AbpDuckDBOptions>(options =>
{
    options.EnableMultiTenancy = true;
    options.TenantDatabaseNamePattern = "tenant_{0}"; // {0} 将被替换为租户ID
});
```

### 多租户仓储使用

```csharp
// 自动识别当前租户上下文
public class TenantSpecificService : ITransientDependency
{
    private readonly IDuckDBRepository<CustomerData> _customerRepository;
    private readonly ICurrentTenant _currentTenant;
    
    public TenantSpecificService(IDuckDBRepository<CustomerData> customerRepository, ICurrentTenant currentTenant)
    {
        _customerRepository = customerRepository;
        _currentTenant = currentTenant;
    }
    
    public async Task<List<CustomerData>> GetCustomersAsync()
    {
        // 自动在当前租户的数据库中查询
        return await _customerRepository.QueryAsync();
    }
}
```

## Parquet 文件处理

DuckDB 的一大优势是高效处理 Parquet 文件，Abp.DuckDB 提供了简单的 API：

```csharp
public class DataAnalysisService : ITransientDependency
{
    private readonly IDuckDBRepository<SalesData> _repository;
    
    public DataAnalysisService(IDuckDBRepository<SalesData> repository)
    {
        _repository = repository;
    }
    
    public async Task<List<SalesData>> GetSalesDataAsync(int year, int month)
    {
        // 按月份查询 Parquet 文件
        return (await _repository.QueryByMonthAsync<SalesData>(
            parquetDirectory: "Data/Sales",
            year: year,
            month: month
        )).Items;
    }
    
    public async Task ExportToParquetAsync(string tableName, string exportPath)
    {
        await _repository.ExportDataToParquetAsync(tableName, exportPath);
    }
}
```

## 高级功能

### 资源监控和限制

```csharp
public class ResourceMonitoringService : ITransientDependency
{
    private readonly ITenantResourceMonitor _resourceMonitor;
    
    public ResourceMonitoringService(ITenantResourceMonitor resourceMonitor)
    {
        _resourceMonitor = resourceMonitor;
    }
    
    public void ConfigureTenantLimits(string tenantId)
    {
        _resourceMonitor.SetTenantConfig(tenantId, new TenantResourceConfig
        {
            QueryTimeoutMs = 60000,
            MaxQueriesPerSecond = 100,
            MaxResultSize = 50000
        });
    }
    
    public TenantResourceUsage GetTenantUsage(string tenantId)
    {
        return _resourceMonitor.GetResourceUsage(tenantId);
    }
}
```

### 租户数据库管理

```csharp
public class TenantDatabaseAdminService : ITransientDependency
{
    private readonly ITenantDatabaseService _databaseService;
    
    public TenantDatabaseAdminService(ITenantDatabaseService databaseService)
    {
        _databaseService = databaseService;
    }
    
    public async Task BackupTenantDatabaseAsync(string tenantId)
    {
        string backupPath = await _databaseService.BackupTenantDatabaseAsync(tenantId);
        Console.WriteLine($"备份已创建：{backupPath}");
    }
    
    public async Task RestoreTenantDatabaseAsync(string tenantId, string backupPath)
    {
        await _databaseService.RestoreTenantDatabaseAsync(tenantId, backupPath);
    }
}
```

## 性能优化

Abp.DuckDB 采用以下技术确保最佳性能：

- 惰性连接：仅在需要时创建数据库连接
- 连接池管理：重用连接以减少开销
- 批量操作：支持高效大批量数据操作
- 参数化查询：避免 SQL 注入同时提高性能
- 自动重试机制：透明处理临时并发问题

## 贡献

欢迎贡献！请参阅 [贡献指南](CONTRIBUTING.md)。

## 许可证

本项目采用 MIT 许可证 - 详细信息请查看 [LICENSE](LICENSE) 文件。

## 致谢

- [DuckDB](https://duckdb.org/) - 高性能分析型数据库
- [DuckDB.NET](https://github.com/Giorgi/DuckDB.NET) - C# DuckDB 绑定
- [ABP 框架](https://github.com/aspnetboilerplate/aspnetboilerplate)) - 完整的 Web 应用框架