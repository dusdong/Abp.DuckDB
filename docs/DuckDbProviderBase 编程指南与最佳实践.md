# DuckDbProviderBase 编程指南与最佳实践

## 概述

`DuckDbProviderBase` 是与 DuckDB 数据库交互的基础抽象类，提供连接管理、查询执行和资源释放等核心功能。这个类实现了 `IDuckDBProvider` 接口，是所有 DuckDB 提供程序的基础。

## 主要功能

- 连接池管理
- 查询执行和参数处理
- 事务管理
- 资源释放和清理
- 性能监控和日志记录
- 语句缓存和预编译

## 初始化和配置

### 基本初始化

```csharp
// 使用配置对象初始化
var config = new DuckDBConfiguration { 
    ConnectionString = "your_connection_string",
    ThreadCount = 4,
    MemoryLimit = "4GB" 
};
provider.Initialize(config);

// 或直接使用连接字符串
provider.Initialize("your_connection_string");
```

### 连接池配置

```csharp
var config = new DuckDBConfiguration {
    UseConnectionPool = true,
    MinConnections = 2,
    MaxConnections = 10,
    MaxIdleTimeSeconds = 300,
    MaxConnectionLifetimeHours = 6
};
provider.Initialize(config);
```

### 性能监控配置

```csharp
var config = new DuckDBConfiguration {
    EnablePerformanceMonitoring = true,
    LogSlowQueries = true,
    SlowQueryThresholdMs = 500
};
provider.Initialize(config);
```

## 查询执行

### 执行非查询语句

```csharp
// 执行非查询语句
int affectedRows = await provider.ExecuteNonQueryAsync("CREATE TABLE users (id INT, name VARCHAR)");

// 带参数的非查询语句
await provider.ExecuteNonQueryAsync("INSERT INTO users VALUES ($1, $2)", 1, "John Doe");
```

### 执行标量查询

```csharp
// 执行标量查询
int count = await provider.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM users");

// 带参数的标量查询
string name = await provider.ExecuteScalarAsync<string>("SELECT name FROM users WHERE id = $1", 1);
```

### 执行对象查询

```csharp
// 使用对象查询
var users = await provider.QueryWithRawSqlAsync<User>("SELECT * FROM users");

// 带参数的对象查询
var specificUsers = await provider.QueryWithRawSqlAsync<User>("SELECT * FROM users WHERE age > $1", 25);
```

### 使用表达式查询

```csharp
// 使用表达式查询
var results = await provider.QueryWithLimitOffsetAsync<User>(
    u => u.Age > 25,
    limit: 10,
    offset: 0,
    orderByColumn: "Name",
    ascending: true
);
```

## 批量操作

```csharp
// 批量插入示例
var users = GetManyUsers(); // 获取多个用户记录
var batchSize = 1000;

await provider.ExecuteBatchOperationAsync(
    "批量插入用户",
    users,
    async batch => {
        // 为每批执行插入操作
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO users VALUES ($1, $2)";
        
        foreach (var user in batch) {
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new DuckDBParameter { Value = user.Id });
            cmd.Parameters.Add(new DuckDBParameter { Value = user.Name });
            await cmd.ExecuteNonQueryAsync();
        }
        
        return batch.Count; // 返回处理的记录数
    },
    batchSize
);
```

## 性能优化

### 应用优化设置

```csharp
// 应用优化设置
await provider.ApplyOptimizationAsync();
```

### 预热元数据缓存

```csharp
// 预热常用实体的元数据缓存
provider.PrewarmEntityMetadata(typeof(User), typeof(Product), typeof(Order));
```

### 缓存管理

```csharp
// 获取缓存统计信息
string statistics = provider.GetCacheStatistics();

// 手动清理部分缓存
provider.CleanupCache(evictionPercentage: 25);

// 清空所有缓存
provider.ClearAllCaches();
```

## 最佳实践

1. **使用连接池**
    - 在高并发环境下启用连接池以提高性能
    - 根据工作负载调整最小和最大连接数

2. **适当配置线程数**
    - `ThreadCount` 设置为服务器核心数或稍高
    - 避免设置过高，会导致过度上下文切换

3. **合理设置内存限制**
    - `MemoryLimit` 设置为系统可用内存的一个合理部分
    - 过低会导致性能下降，过高可能导致系统内存不足

4. **利用语句缓存**
    - 对频繁执行的查询，启用语句缓存
    - 对动态生成或仅执行一次的查询，禁用缓存

5. **使用参数化查询**
    - 防止SQL注入
    - 提高查询重用和缓存命中率

6. **正确处理资源释放**
    - 使用 `using` 语句或手动调用 `Dispose()`
    - 避免连接泄漏

7. **错误处理**
    - 捕获并适当处理 DuckDB 异常
    - 在生产环境中避免将详细错误信息暴露给最终用户

8. **批量操作处理**
    - 对大量数据操作使用批处理
    - 根据数据大小和系统内存调整批处理大小

## 常见问题与解决方法

### 连接问题

**问题**: 无法连接到DuckDB
**解决**:
- 检查连接字符串格式
- 确认文件路径存在且有访问权限
- 检查是否有其他进程锁定了数据库文件

### 性能问题

**问题**: 查询执行缓慢
**解决**:
- 启用性能监控，分析慢查询
- 增加线程数和内存限制
- 检查查询计划，优化SQL
- 对经常查询的数据创建索引

### 内存问题

**问题**: 系统内存占用过高
**解决**:
- 降低DuckDB内存限制
- 减少线程数
- 使用批处理处理大量数据
- 定期清理缓存

### 资源泄漏

**问题**: 连接或其他资源未释放
**解决**:
- 检查是否所有使用实例都被正确释放
- 使用连接池管理连接生命周期
- 设置连接超时和生命周期限制
