using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Castle.Core;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.Repository;

/// <summary>
/// DuckDB 仓储基类，封装数据库操作，包括连接管理、事务管理和命令执行。
/// 基于 DuckDbProviderBase 实现，提供更高级的仓储功能。
/// </summary>
public abstract class DuckDBRepositoryBase : IInitializable, IDisposable
{
    /// <summary>
    /// 获取数据库连接字符串。具体仓储类需要实现此属性。
    /// </summary>
    public abstract string ConnectionString { get; }

    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// 连接命令超时（秒）
    /// </summary>
    public int CommandTimeoutSeconds { get; set; } = 30;

    #region Private Fields

    private readonly IDuckDBProvider _provider;
    private readonly ILogger _logger;
    private readonly SqlBuilder _sqlBuilder;
    private bool _disposed = false;
    private bool _initialized = false;
    private readonly Regex _identifierPattern = new Regex(@"^[a-zA-Z0-9_]+$", RegexOptions.Compiled);

    // 缓存表结构以提高性能
    private readonly Dictionary<Type, TableInfo> _tableCache = new Dictionary<Type, TableInfo>();
    private readonly object _tableCacheLock = new object();

    #endregion

    #region Constructor

    /// <summary>
    /// 使用指定的DuckDB提供程序和日志记录器创建 DuckDBRepository 实例。
    /// </summary>
    /// <param name="provider">DuckDB提供程序</param>
    /// <param name="logger">日志记录器</param>
    public DuckDBRepositoryBase(IDuckDBProvider provider, ILogger logger)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _sqlBuilder = new SqlBuilder(logger);
    }

    /// <summary>
    /// 初始化仓储
    /// </summary>
    public void Initialize()
    {
        try
        {
            if (_initialized)
            {
                _logger.Warn("仓储已经初始化，忽略重复调用");
                return;
            }

            // 使用提供程序初始化连接
            var config = new DuckDBConfiguration
            {
                ConnectionString = ConnectionString,
                CommandTimeout = TimeSpan.FromSeconds(CommandTimeoutSeconds),
                MaxRetryCount = MaxRetryAttempts
            };

            _provider.Initialize(config);
            _initialized = true;

            _logger.Info($"DuckDB仓储初始化完成，连接字符串: {ConnectionString}");
        }
        catch (Exception ex)
        {
            _logger.Error($"DuckDB仓储初始化失败: {ex.Message}", ex);
            throw;
        }
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// 验证SQL标识符是否合法
    /// </summary>
    private string ValidateIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || !_identifierPattern.IsMatch(name))
            throw new ArgumentException($"无效的SQL标识符: {name}");
        return name;
    }

    /// <summary>
    /// 获取表信息
    /// </summary>
    private TableInfo GetTableInfo<TEntity>()
    {
        var entityType = typeof(TEntity);

        lock (_tableCacheLock)
        {
            if (!_tableCache.TryGetValue(entityType, out var tableInfo))
            {
                var tableAttribute = entityType.GetCustomAttribute<TableAttribute>();
                if (tableAttribute == null)
                    throw new InvalidOperationException($"实体类 {entityType.Name} 缺少 TableAttribute");

                var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => !Attribute.IsDefined(p, typeof(NotMappedAttribute)))
                    .ToList();

                var columns = properties.Select(p =>
                {
                    var columnAttr = p.GetCustomAttribute<ColumnAttribute>();
                    return new ColumnInfo
                    {
                        PropertyInfo = p,
                        Name = columnAttr?.Name ?? p.Name,
                        IsPrimaryKey = columnAttr?.IsPrimaryKey ?? false,
                        IsAutoIncrement = columnAttr?.IsAutoIncrement ?? false,
                        IsNullable = columnAttr?.IsNullable ?? IsNullableType(p)
                    };
                }).ToList();

                tableInfo = new TableInfo
                {
                    Name = tableAttribute.Name,
                    Properties = properties,
                    Columns = columns,
                    PrimaryKey = columns.FirstOrDefault(c => c.IsPrimaryKey)
                };

                _tableCache[entityType] = tableInfo;
            }

            return tableInfo;
        }
    }

    /// <summary>
    /// 判断属性是否可为空
    /// </summary>
    private bool IsNullableType(PropertyInfo prop)
    {
        if (!prop.PropertyType.IsValueType)
            return true; // 引用类型默认可为空

        if (Nullable.GetUnderlyingType(prop.PropertyType) != null)
            return true; // 可空值类型

        return false; // 值类型不可为空
    }

    #endregion

    #region Transaction Methods

    /// <summary>
    /// 使用事务执行操作
    /// </summary>
    public async Task<T> WithTransactionAsync<T>(Func<DuckDBTransaction, Task<T>> action)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (action == null) throw new ArgumentNullException(nameof(action));

        var connection = _provider.GetDuckDBConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            var result = await action(transaction).ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.Error($"事务执行失败：{ex.Message}", ex);
            await transaction.RollbackAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// 使用事务执行操作（无返回值）
    /// </summary>
    public async Task WithTransactionAsync(Func<DuckDBTransaction, Task> action)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (action == null) throw new ArgumentNullException(nameof(action));

        var connection = _provider.GetDuckDBConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            await action(transaction).ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.Error($"事务执行失败：{ex.Message}", ex);
            await transaction.RollbackAsync().ConfigureAwait(false);
            throw;
        }
    }

    #endregion

    #region Table Creation

    /// <summary>
    /// 创建表（如果不存在）
    /// </summary>
    public async Task<int> CreateTableAsync<T>()
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");

        try
        {
            // 生成创建表的脚本
            var createTableScript = TableCreationHelper.GenerateCreateTableScript<T>();
            _logger.Debug($"执行建表SQL: {createTableScript}");

            // 使用provider执行SQL
            return await _provider.ExecuteNonQueryAsync(createTableScript).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.Error($"创建表 {typeof(T).Name} 失败: {ex.Message}", ex);
            throw;
        }
    }

    #endregion

    #region Insert Methods

    /// <summary>
    /// 异步插入单个实体
    /// </summary>
    public async Task<int> InsertAsync<TEntity>(TEntity entity)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var sql = new DuckDBSql().Insert(entity);
        return await _provider.ExecuteNonQueryAsync(sql.GetSql(), sql.GetParameters().ToArray()).ConfigureAwait(false);
    }

    /// <summary>
    /// 异步批量插入多个实体
    /// </summary>
    public async Task<int> InsertBatchAsync<TEntity>(IEnumerable<TEntity> entities, int batchSize = 500)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var entitiesList = entities.ToList();
        if (entitiesList.Count == 0) return 0;

        int totalAffected = 0;

        // 分批次插入，避免参数过多
        for (int i = 0; i < entitiesList.Count; i += batchSize)
        {
            var batch = entitiesList.Skip(i).Take(batchSize).ToList();
            var sql = new DuckDBSql().Insert(batch);

            int result = await _provider.ExecuteNonQueryAsync(sql.GetSql(), sql.GetParameters().ToArray())
                .ConfigureAwait(false);

            totalAffected += result;
        }

        return totalAffected;
    }

    #endregion

    #region Update Methods

    /// <summary>
    /// 异步更新实体
    /// </summary>
    public async Task<int> UpdateAsync<TEntity>(TEntity entity, Expression<Func<TEntity, bool>> predicate)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var sql = new DuckDBSql().Update(entity, predicate);
        return await _provider.ExecuteNonQueryAsync(sql.GetSql(), sql.GetParameters().ToArray())
            .ConfigureAwait(false);
    }

    #endregion

    #region Delete Methods

    /// <summary>
    /// 异步删除实体
    /// </summary>
    public async Task<int> DeleteAsync<TEntity>(Expression<Func<TEntity, bool>> predicate)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var sql = new DuckDBSql().Delete<TEntity>(predicate);
        return await _provider.ExecuteNonQueryAsync(sql.GetSql(), sql.GetParameters().ToArray())
            .ConfigureAwait(false);
    }

    #endregion

    #region Query Methods

    /// <summary>
    /// 异步查询实体列表
    /// </summary>
    public async Task<List<TEntity>> QueryAsync<TEntity>(Expression<Func<TEntity, bool>> predicate = null)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");

        var sql = new DuckDBSql().Select<TEntity>(predicate);
        return await _provider.QueryWithRawSqlAsync<TEntity>(sql.GetSql(), sql.GetParameters().ToArray())
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 异步分页查询实体列表
    /// </summary>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = null,
        bool ascending = true)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");

        if (pageIndex < 1)
            throw new ArgumentException("页码必须大于或等于1", nameof(pageIndex));
        if (pageSize < 1)
            throw new ArgumentException("每页记录数必须大于或等于1", nameof(pageSize));

        // 先获取总数
        var countSql = new DuckDBSql().Count<TEntity>(predicate);
        var countResult = await _provider.ExecuteScalarAsync<int>(countSql.GetSql(), countSql.GetParameters().ToArray())
            .ConfigureAwait(false);

        if (countResult == 0)
            return (new List<TEntity>(), 0);

        // 查询分页数据
        var sql = new DuckDBSql().Select<TEntity>(predicate);

        if (!string.IsNullOrWhiteSpace(orderByColumn))
            sql.OrderBy(ValidateIdentifier(orderByColumn), ascending);

        sql.Limit(pageSize).Offset((pageIndex - 1) * pageSize);

        var items = await _provider.QueryWithRawSqlAsync<TEntity>(sql.GetSql(), sql.GetParameters().ToArray())
            .ConfigureAwait(false);

        return (items, countResult);
    }

    /// <summary>
    /// 根据自定义SQL查询数据
    /// </summary>
    public async Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, params object[] parameters)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentNullException(nameof(sql));

        // 转换为DuckDBParameter数组
        var duckParams = parameters.Select((p, i) => new DuckDBParameter
        {
            ParameterName = $"p{i}",
            Value = p ?? DBNull.Value
        }).ToArray();

        return await _provider.QueryWithRawSqlAsync<TEntity>(sql, duckParams).ConfigureAwait(false);
    }

    /// <summary>
    /// 根据主键获取实体
    /// </summary>
    public async Task<TEntity> GetByIdAsync<TEntity, TKey>(TKey id)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");

        var tableInfo = GetTableInfo<TEntity>();
        if (tableInfo.PrimaryKey == null)
            throw new InvalidOperationException($"实体 {typeof(TEntity).Name} 未定义主键列");

        // 构建主键查询
        var parameter = Expression.Parameter(typeof(TEntity), "e");
        var property = Expression.Property(parameter, tableInfo.PrimaryKey.PropertyInfo);
        var constant = Expression.Constant(id);
        var equality = Expression.Equal(property, constant);
        var lambda = Expression.Lambda<Func<TEntity, bool>>(equality, parameter);

        var result = await QueryAsync(lambda).ConfigureAwait(false);
        return result.FirstOrDefault();
    }

    #endregion

    #region Count and Sum Methods

    /// <summary>
    /// 异步获取实体的记录数
    /// </summary>
    public async Task<int> CountAsync<TEntity>(Expression<Func<TEntity, bool>> predicate = null)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");

        var sql = new DuckDBSql().Count<TEntity>(predicate);
        return await _provider.ExecuteScalarAsync<int>(sql.GetSql(), sql.GetParameters().ToArray())
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 异步获取实体某列的总和
    /// </summary>
    public async Task<decimal> SumAsync<TEntity>(Expression<Func<TEntity, decimal>> selector,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        if (!_initialized) throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");
        if (selector == null) throw new ArgumentNullException(nameof(selector));

        var sql = new DuckDBSql().Sum<TEntity>(selector, predicate);
        var result = await _provider.ExecuteScalarAsync<object>(sql.GetSql(), sql.GetParameters().ToArray())
            .ConfigureAwait(false);

        return result == null || result == DBNull.Value ? 0 : Convert.ToDecimal(result);
    }

    #endregion

    #region IDisposable Implementation

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 释放托管和非托管资源
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                // 仅在需要时释放托管资源
                if (_provider is IDisposable disposableProvider)
                {
                    disposableProvider.Dispose();
                }
            }

            _disposed = true;
        }
    }

    #endregion

    #region Supporting Classes

    /// <summary>
    /// 表信息
    /// </summary>
    private class TableInfo
    {
        public string Name { get; set; }
        public List<PropertyInfo> Properties { get; set; }
        public List<ColumnInfo> Columns { get; set; }
        public ColumnInfo PrimaryKey { get; set; }
    }

    /// <summary>
    /// 列信息
    /// </summary>
    private class ColumnInfo
    {
        public PropertyInfo PropertyInfo { get; set; }
        public string Name { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsAutoIncrement { get; set; }
        public bool IsNullable { get; set; }
    }

    #endregion
}
