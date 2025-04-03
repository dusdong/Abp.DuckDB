using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Castle.Core;
using Castle.Core.Logging;
using DuckDB.NET.Data;

namespace Abp.DuckDB.Repository;

/// <summary>
/// DuckDB 仓储类，封装数据库操作，包括连接管理、事务管理和命令执行。
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
    /// 初始重试延迟（毫秒）
    /// </summary>
    public int InitialRetryDelayMs { get; set; } = 100;

    /// <summary>
    /// 连接命令超时（秒）
    /// </summary>
    public int CommandTimeoutSeconds { get; set; } = 30;

    #region ctor

    private DuckDBConnection _connection;
    private readonly object _connectionLock = new object();
    private readonly ILogger _logger;
    private bool _disposed = false;
    private bool _connectionInitialized = false;
    private readonly Regex _identifierPattern = new Regex(@"^[a-zA-Z0-9_]+$", RegexOptions.Compiled);
    private readonly HashSet<DuckDBTransaction> _activeTransactions = new HashSet<DuckDBTransaction>();
    private readonly object _transactionsLock = new object();

    /// <summary>
    /// 使用指定的连接字符串和日志记录器创建 DuckDBRepository 实例。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    public DuckDBRepositoryBase(ILogger logger)
    {
        _logger = logger;
    }

    public void Initialize()
    {
        try
        {
            // 仅初始化状态，不立即创建连接
            _connectionInitialized = true;
            _logger.Info($"DuckDB仓储初始化完成，连接字符串: {ConnectionString}");
        }
        catch (Exception ex)
        {
            _logger.Error($"DuckDB仓储初始化失败: {ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 惰性获取连接，如果连接不存在或已关闭则创建新连接
    /// </summary>
    private DuckDBConnection GetConnection()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DuckDBRepositoryBase));

        if (!_connectionInitialized)
            throw new InvalidOperationException("仓储未初始化，请先调用Initialize方法");

        if (_connection == null || _connection.State != System.Data.ConnectionState.Open)
        {
            lock (_connectionLock)
            {
                if (_connection == null || _connection.State != System.Data.ConnectionState.Open)
                {
                    try
                    {
                        // 释放旧连接（如果存在）
                        if (_connection != null)
                        {
                            try
                            {
                                _connection.Close();
                                _connection.Dispose();
                            }
                            catch (Exception ex)
                            {
                                _logger.Warn($"关闭旧连接时发生错误: {ex.Message}");
                                // 继续创建新连接，不抛出异常
                            }
                        }

                        // 创建新连接
                        _connection = new DuckDBConnection(ConnectionString);
                        _connection.Open();
                        _logger.Debug("DuckDB连接已创建并打开");
                    }
                    catch (Exception ex)
                    {
                        _logger.Error($"创建DuckDB连接失败: {ex.Message}", ex);
                        throw;
                    }
                }
            }
        }

        return _connection;
    }

    #endregion

    /// <summary>
    /// 获取数据库连接字符串。
    /// </summary>
    public string GetConnectionString(string databaseFilePath)
    {
        return $"Data Source={databaseFilePath}";
    }

    /// <summary>
    /// 验证SQL标识符是否合法
    /// </summary>
    private string ValidateIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || !_identifierPattern.IsMatch(name))
            throw new ArgumentException($"无效的SQL标识符: {name}");
        return name;
    }

    #region Transaction Management

    /// <summary>
    /// 开始一个新事务并跟踪它
    /// </summary>
    public DuckDBTransaction BeginTransaction()
    {
        var connection = GetConnection();
        var transaction = connection.BeginTransaction();

        lock (_transactionsLock)
        {
            _activeTransactions.Add(transaction);
        }

        return transaction;
    }

    /// <summary>
    /// 从活跃事务中移除
    /// </summary>
    private void RemoveTransaction(DuckDBTransaction transaction)
    {
        lock (_transactionsLock)
        {
            _activeTransactions.Remove(transaction);
        }
    }

    #endregion

    #region Table Creation Method

    /// <summary>
    /// 创建表（如果不存在）。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    public async Task<int> CreateTableAsync<T>()
    {
        try
        {
            // 生成创建表的脚本
            var createTableScript = TableCreationHelper.GenerateCreateTableScript<T>();

            using var command = GetConnection().CreateCommand();
            command.CommandText = createTableScript;
            command.CommandTimeout = CommandTimeoutSeconds;

            _logger.Debug($"执行建表SQL: {createTableScript}");
            return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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
    /// 异步插入单个实体。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="entity">要插入的实体对象。</param>
    /// <returns>受影响的行数。</returns>
    public async Task<int> InsertAsync<TEntity>(TEntity entity)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        var sql = new DuckDBSql().Insert(entity);
        return await ExecuteNonQueryAsync(sql).ConfigureAwait(false);
    }

    /// <summary>
    /// 异步插入多个实体（批量插入）。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="entities">要插入的实体集合。</param>
    /// <param name="batchSize">每批处理的实体数量，避免参数过多。</param>
    /// <returns>受影响的行数。</returns>
    public async Task<int> InsertBatchAsync<TEntity>(IEnumerable<TEntity> entities, int batchSize = 500)
    {
        if (entities == null)
            throw new ArgumentNullException(nameof(entities));

        var entitiesList = entities.ToList();
        if (entitiesList.Count == 0)
            return 0;

        int totalAffected = 0;

        // 分批次插入，避免参数过多
        for (int i = 0; i < entitiesList.Count; i += batchSize)
        {
            var batch = entitiesList.Skip(i).Take(batchSize).ToList();
            var sql = new DuckDBSql().Insert(batch);
            totalAffected += await ExecuteNonQueryAsync(sql).ConfigureAwait(false);
        }

        return totalAffected;
    }

    #endregion

    #region Update Methods

    /// <summary>
    /// 异步更新实体。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="entity">包含要更新的数据的实体对象。</param>
    /// <param name="predicate">更新条件表达式。</param>
    /// <returns>受影响的行数。</returns>
    public async Task<int> UpdateAsync<TEntity>(TEntity entity, Expression<Func<TEntity, bool>> predicate)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        var sql = new DuckDBSql().Update(entity, predicate);
        return await ExecuteNonQueryAsync(sql).ConfigureAwait(false);
    }

    #endregion

    #region Delete Methods

    /// <summary>
    /// 异步删除实体。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="predicate">删除条件表达式。</param>
    /// <returns>受影响的行数。</returns>
    public async Task<int> DeleteAsync<TEntity>(Expression<Func<TEntity, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        var sql = new DuckDBSql().Delete<TEntity>(predicate);
        return await ExecuteNonQueryAsync(sql).ConfigureAwait(false);
    }

    #endregion

    #region Query Methods

    /// <summary>
    /// 异步查询实体列表。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="predicate">查询条件表达式，可选。</param>
    /// <returns>实体列表。</returns>
    public async Task<List<TEntity>> QueryAsync<TEntity>(Expression<Func<TEntity, bool>> predicate = null)
    {
        var sql = new DuckDBSql().Select(predicate);
        return await ExecuteQueryAsync<TEntity>(sql).ConfigureAwait(false);
    }

    /// <summary>
    /// 异步分页查询实体列表。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="predicate">查询条件表达式，可选。</param>
    /// <param name="pageIndex">页码，从1开始。</param>
    /// <param name="pageSize">每页记录数。</param>
    /// <param name="orderByColumn">排序列名。</param>
    /// <param name="ascending">是否升序排序。</param>
    /// <returns>包含分页结果和总记录数的元组。</returns>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryPagedAsync<TEntity>(
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = null,
        bool ascending = true)
    {
        if (pageIndex < 1)
            throw new ArgumentException("页码必须大于或等于1。", nameof(pageIndex));
        if (pageSize < 1)
            throw new ArgumentException("每页记录数必须大于或等于1。", nameof(pageSize));

        // 先获取总数
        var countSql = new DuckDBSql().Count(predicate);

        using var countCommand = GetConnection().CreateCommand();
        countCommand.CommandText = countSql.GetSql();
        countCommand.Parameters.AddRange(countSql.GetParameters());
        countCommand.CommandTimeout = CommandTimeoutSeconds;

        var countResult = await countCommand.ExecuteScalarAsync().ConfigureAwait(false);
        int totalCount = Convert.ToInt32(countResult);

        if (totalCount == 0)
            return (new List<TEntity>(), 0);

        // 构建分页查询SQL
        var sql = new DuckDBSql().Select(predicate);

        // 添加排序
        if (!string.IsNullOrWhiteSpace(orderByColumn))
            sql.OrderBy(ValidateIdentifier(orderByColumn), ascending);

        // 添加分页
        sql.Limit(pageSize).Offset((pageIndex - 1) * pageSize);

        var items = await ExecuteQueryAsync<TEntity>(sql).ConfigureAwait(false);

        return (items, totalCount);
    }

    /// <summary>
    /// 根据自定义SQL查询数据
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="sql">SQL查询字符串</param>
    /// <param name="parameters">SQL参数</param>
    /// <returns>实体列表</returns>
    public async Task<List<TEntity>> QueryWithRawSqlAsync<TEntity>(string sql, params DuckDBParameter[] parameters)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentNullException(nameof(sql));

        using var command = GetConnection().CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = CommandTimeoutSeconds;

        if (parameters != null && parameters.Length > 0)
            command.Parameters.AddRange(parameters);

        return await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
    }

    /// <summary>
    /// 根据主键获取实体
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <typeparam name="TKey">主键类型</typeparam>
    /// <param name="id">主键值</param>
    /// <returns>实体对象，如果未找到则返回null</returns>
    public async Task<TEntity> GetByIdAsync<TEntity, TKey>(TKey id)
    {
        var type = typeof(TEntity);
        var props = type.GetProperties();
        var pkProp = props.FirstOrDefault(p =>
            p.GetCustomAttribute<ColumnAttribute>()?.IsPrimaryKey ?? false);

        if (pkProp == null)
            throw new InvalidOperationException($"实体 {type.Name} 未定义主键列");

        var columnName = pkProp.GetCustomAttribute<ColumnAttribute>()?.Name ?? pkProp.Name;

        // 使用表达式树构建谓词
        var parameter = Expression.Parameter(type, "e");
        var property = Expression.Property(parameter, pkProp);
        var constant = Expression.Constant(id);
        var equality = Expression.Equal(property, constant);
        var lambda = Expression.Lambda<Func<TEntity, bool>>(equality, parameter);

        var result = await QueryAsync(lambda).ConfigureAwait(false);
        return result.FirstOrDefault();
    }

    #endregion

    #region Parquet Methods

    /// <summary>
    /// 异步查询 Parquet 文件，返回实体列表。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="parquetFilePaths">Parquet 文件路径列表。</param>
    /// <param name="predicate">查询条件表达式，可选。</param>
    /// <returns>实体列表。</returns>
    public async Task<List<TEntity>> QueryParquetFilesAsync<TEntity>(
        IEnumerable<string> parquetFilePaths,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        if (parquetFilePaths == null || !parquetFilePaths.Any())
            throw new ArgumentNullException(nameof(parquetFilePaths));

        var sql = new DuckDBSql().SelectFromParquet<TEntity>(parquetFilePaths, predicate);
        return await ExecuteQueryAsync<TEntity>(sql).ConfigureAwait(false);
    }

    /// <summary>
    /// 分页查询Parquet文件
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="parquetFilePaths">Parquet文件路径列表</param>
    /// <param name="predicate">查询条件表达式，可选</param>
    /// <param name="pageIndex">页码，从1开始</param>
    /// <param name="pageSize">每页记录数</param>
    /// <param name="orderByColumn">排序列</param>
    /// <param name="ascending">是否升序</param>
    /// <returns>包含分页结果和总记录数的元组</returns>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryParquetFilesPagedAsync<TEntity>(
        IEnumerable<string> parquetFilePaths,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = null,
        bool ascending = true)
    {
        if (parquetFilePaths == null || !parquetFilePaths.Any())
            throw new ArgumentNullException(nameof(parquetFilePaths));

        if (pageIndex < 1)
            throw new ArgumentException("页码必须大于或等于1。", nameof(pageIndex));
        if (pageSize < 1)
            throw new ArgumentException("每页记录数必须大于或等于1。", nameof(pageSize));

        // 先获取总数
        var countSql = new DuckDBSql()
            .SelectFromParquet<TEntity>(parquetFilePaths, predicate)
            .Append(" SELECT COUNT(*) FROM combined");

        using var countCommand = GetConnection().CreateCommand();
        countCommand.CommandText = countSql.GetSql();
        countCommand.Parameters.AddRange(countSql.GetParameters());
        countCommand.CommandTimeout = CommandTimeoutSeconds;

        var countResult = await countCommand.ExecuteScalarAsync().ConfigureAwait(false);
        int totalCount = Convert.ToInt32(countResult);

        if (totalCount == 0)
            return (new List<TEntity>(), 0);

        // 查询分页数据
        var sql = new DuckDBSql().SelectFromParquet<TEntity>(parquetFilePaths, predicate);

        if (!string.IsNullOrWhiteSpace(orderByColumn))
            sql.OrderBy(ValidateIdentifier(orderByColumn), ascending);

        sql.Limit(pageSize).Offset((pageIndex - 1) * pageSize);

        var items = await ExecuteQueryAsync<TEntity>(sql).ConfigureAwait(false);

        return (items, totalCount);
    }

    /// <summary>
    /// 导出指定表的数据为 Parquet 文件。
    /// </summary>
    /// <param name="tableName">表名。</param>
    /// <param name="exportFilePath">导出文件路径。</param>
    public async Task ExportDataToParquetAsync(string tableName, string exportFilePath)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        if (string.IsNullOrWhiteSpace(exportFilePath))
            throw new ArgumentNullException(nameof(exportFilePath));

        tableName = ValidateIdentifier(tableName);

        try
        {
            // 确保目录存在
            var directory = Path.GetDirectoryName(exportFilePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            using var command = GetConnection().CreateCommand();
            command.CommandTimeout = CommandTimeoutSeconds;

            // 使用参数化查询防止SQL注入
            command.CommandText = "COPY $tableName TO $filePath (FORMAT 'parquet');";
            command.Parameters.Add(new DuckDBParameter { ParameterName = "tableName", Value = tableName });
            command.Parameters.Add(new DuckDBParameter { ParameterName = "filePath", Value = exportFilePath });

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            _logger.Info($"成功将表 '{tableName}' 导出为 Parquet 文件 '{exportFilePath}'。");
        }
        catch (Exception ex)
        {
            _logger.Error($"导出表 '{tableName}' 失败：{ex.Message}", ex);
            throw;
        }
    }

    /// <summary>
    /// 按月份查询支付数据
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="parquetDirectory">Parquet文件目录</param>
    /// <param name="year">年份</param>
    /// <param name="month">月份</param>
    /// <param name="predicate">查询条件</param>
    /// <param name="pageIndex">页码</param>
    /// <param name="pageSize">每页大小</param>
    /// <param name="orderByColumn">排序列，默认CreateTime</param>
    /// <param name="ascending">是否升序，默认降序</param>
    /// <returns>分页结果</returns>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryByMonthAsync<TEntity>(
        string parquetDirectory,
        int year,
        int month,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = "CreateTime",
        bool ascending = false)
    {
        if (string.IsNullOrWhiteSpace(parquetDirectory))
            throw new ArgumentNullException(nameof(parquetDirectory));

        if (!Directory.Exists(parquetDirectory))
            throw new DirectoryNotFoundException($"找不到目录: {parquetDirectory}");

        // 查找指定年月的文件
        string monthPattern = $"*_{year}_{month.ToString("D2")}_*.parquet";
        var monthFiles = Directory.GetFiles(parquetDirectory, monthPattern, SearchOption.AllDirectories);

        if (monthFiles.Length == 0)
            return (new List<TEntity>(), 0);

        return await QueryParquetFilesPagedAsync<TEntity>(
            monthFiles,
            predicate,
            pageIndex,
            pageSize,
            orderByColumn,
            ascending);
    }

    /// <summary>
    /// 按日期范围查询支付数据
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <param name="parquetDirectory">Parquet文件目录</param>
    /// <param name="startDate">开始日期</param>
    /// <param name="endDate">结束日期</param>
    /// <param name="predicate">查询条件</param>
    /// <param name="pageIndex">页码</param>
    /// <param name="pageSize">每页大小</param>
    /// <param name="orderByColumn">排序列，默认CreateTime</param>
    /// <param name="ascending">是否升序，默认降序</param>
    /// <returns>分页结果</returns>
    public async Task<(List<TEntity> Items, int TotalCount)> QueryByDateRangeAsync<TEntity>(
        string parquetDirectory,
        DateTime startDate,
        DateTime endDate,
        Expression<Func<TEntity, bool>> predicate = null,
        int pageIndex = 1,
        int pageSize = 20,
        string orderByColumn = "CreateTime",
        bool ascending = false)
    {
        if (string.IsNullOrWhiteSpace(parquetDirectory))
            throw new ArgumentNullException(nameof(parquetDirectory));

        if (!Directory.Exists(parquetDirectory))
            throw new DirectoryNotFoundException($"找不到目录: {parquetDirectory}");

        var filteredFiles = new List<string>();

        // 收集所有指定日期范围内的文件
        for (int year = startDate.Year; year <= endDate.Year; year++)
        {
            int startMonth = (year == startDate.Year) ? startDate.Month : 1;
            int endMonth = (year == endDate.Year) ? endDate.Month : 12;

            for (int month = startMonth; month <= endMonth; month++)
            {
                string monthPattern = $"*_{year}_{month.ToString("D2")}_*.parquet";
                var monthFiles = Directory.GetFiles(parquetDirectory, monthPattern, SearchOption.AllDirectories);
                filteredFiles.AddRange(monthFiles);
            }
        }

        if (filteredFiles.Count == 0)
            return (new List<TEntity>(), 0);

        return await QueryParquetFilesPagedAsync<TEntity>(
            filteredFiles,
            predicate,
            pageIndex,
            pageSize,
            orderByColumn,
            ascending);
    }

    #endregion

    #region Count and Sum Methods

    /// <summary>
    /// 异步获取实体的记录数。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="predicate">查询条件表达式，可选。</param>
    /// <returns>记录数。</returns>
    public async Task<int> CountAsync<TEntity>(Expression<Func<TEntity, bool>> predicate = null)
    {
        var sql = new DuckDBSql().Count(predicate);

        using var command = GetConnection().CreateCommand();
        command.CommandText = sql.GetSql();
        command.Parameters.AddRange(sql.GetParameters());
        command.CommandTimeout = CommandTimeoutSeconds;

        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return Convert.ToInt32(result);
    }

    /// <summary>
    /// 异步获取实体某列的总和。
    /// </summary>
    /// <typeparam name="TEntity">实体类型。</typeparam>
    /// <param name="selector">需要求和的属性选择器。</param>
    /// <param name="predicate">查询条件表达式，可选。</param>
    /// <returns>总和。</returns>
    public async Task<decimal> SumAsync<TEntity>(Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        var sql = new DuckDBSql().Sum(selector, predicate);

        using var command = GetConnection().CreateCommand();
        command.CommandText = sql.GetSql();
        command.Parameters.AddRange(sql.GetParameters());
        command.CommandTimeout = CommandTimeoutSeconds;

        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return result == DBNull.Value ? 0 : Convert.ToDecimal(result);
    }

    /// <summary>
    /// 获取Parquet文件的记录数
    /// </summary>
    public async Task<int> CountParquetFilesAsync<TEntity>(
        IEnumerable<string> parquetFilePaths,
        Expression<Func<TEntity, bool>> predicate = null)
    {
        if (parquetFilePaths == null || !parquetFilePaths.Any())
            throw new ArgumentNullException(nameof(parquetFilePaths));

        var sql = new DuckDBSql()
            .SelectFromParquet<TEntity>(parquetFilePaths, predicate)
            .Append(" SELECT COUNT(*) FROM combined");

        using var command = GetConnection().CreateCommand();
        command.CommandText = sql.GetSql();
        command.Parameters.AddRange(sql.GetParameters());
        command.CommandTimeout = CommandTimeoutSeconds;

        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return Convert.ToInt32(result);
    }

    #endregion

    #region Transaction Methods

    /// <summary>
    /// 使用事务执行操作
    /// </summary>
    public async Task<T> WithTransactionAsync<T>(Func<DuckDBTransaction, Task<T>> action)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        var transaction = BeginTransaction();
        try
        {
            var result = await action(transaction).ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
            RemoveTransaction(transaction);
            return result;
        }
        catch (Exception ex)
        {
            _logger.Error($"事务执行失败：{ex.Message}", ex);
            await transaction.RollbackAsync().ConfigureAwait(false);
            RemoveTransaction(transaction);
            throw;
        }
        finally
        {
            transaction.Dispose();
        }
    }

    /// <summary>
    /// 使用事务执行操作
    /// </summary>
    public async Task WithTransactionAsync(Func<DuckDBTransaction, Task> action)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        var transaction = BeginTransaction();
        try
        {
            await action(transaction).ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
            RemoveTransaction(transaction);
        }
        catch (Exception ex)
        {
            _logger.Error($"事务执行失败：{ex.Message}", ex);
            await transaction.RollbackAsync().ConfigureAwait(false);
            RemoveTransaction(transaction);
            throw;
        }
        finally
        {
            transaction.Dispose();
        }
    }

    #endregion

    #region Private Helper Methods

    /// <summary>
    /// 执行非查询命令（如插入、更新、删除）。
    /// </summary>
    /// <param name="sql">包含 SQL 语句和参数的 DuckDBSql 对象。</param>
    /// <param name="externalTransaction">外部事务，如果为null则创建新事务</param>
    /// <returns>受影响的行数。</returns>
    private async Task<int> ExecuteNonQueryAsync(DuckDBSql sql, DuckDBTransaction externalTransaction = null)
    {
        int attempts = 0;

        while (true)
        {
            attempts++;
            bool ownTransaction = externalTransaction == null;
            DuckDBTransaction transaction = null;

            try
            {
                var connection = GetConnection();

                transaction = ownTransaction ? BeginTransaction() : externalTransaction;

                using var command = connection.CreateCommand();
                if (transaction != null)
                    command.Transaction = transaction;

                command.CommandText = sql.GetSql();
                command.Parameters.AddRange(sql.GetParameters());
                command.CommandTimeout = CommandTimeoutSeconds;

                _logger.Debug($"执行SQL: {command.CommandText}");
                var result = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                if (ownTransaction)
                {
                    await transaction.CommitAsync().ConfigureAwait(false);
                    RemoveTransaction(transaction);
                }

                return result;
            }
            catch (DuckDBException ex) when (
                IsConcurrencyException(ex) &&
                attempts < MaxRetryAttempts &&
                externalTransaction == null) // 只在没有外部事务时重试
            {
                // 并发冲突处理
                if (transaction != null && ownTransaction)
                {
                    try
                    {
                        await transaction.RollbackAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        RemoveTransaction(transaction);
                        transaction.Dispose();
                    }
                }

                // 使用指数退避策略
                int delay = (int)Math.Pow(2, attempts - 1) * InitialRetryDelayMs;
                _logger.Warn($"检测到并发冲突，{attempts}/{MaxRetryAttempts}次重试, 延迟{delay}ms: {ex.Message}");
                await Task.Delay(delay);
                continue;
            }
            catch (Exception ex)
            {
                if (transaction != null && ownTransaction)
                {
                    try
                    {
                        await transaction.RollbackAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        RemoveTransaction(transaction);
                        transaction.Dispose();
                    }
                }

                _logger.Error($"执行数据库操作时发生错误: {ex.Message}", ex);
                throw;
            }
        }
    }

    /// <summary>
    /// 识别并发冲突异常
    /// </summary>
    private bool IsConcurrencyException(Exception ex)
    {
        // 根据DuckDB的错误消息模式识别并发冲突
        string message = ex.Message.ToLowerInvariant();
        return message.Contains("conflict") ||
               message.Contains("constraint violation") ||
               message.Contains("concurrent update") ||
               message.Contains("transaction conflict") ||
               message.Contains("lock");
    }

    /// <summary>
    /// 执行查询命令，返回实体列表。
    /// </summary>
    private async Task<List<TEntity>> ExecuteQueryAsync<TEntity>(DuckDBSql sql)
    {
        using var command = GetConnection().CreateCommand();
        command.CommandText = sql.GetSql();
        command.Parameters.AddRange(sql.GetParameters());
        command.CommandTimeout = CommandTimeoutSeconds;

        _logger.Debug($"执行查询: {command.CommandText}");
        return await ExecuteReaderAsync<TEntity>(command).ConfigureAwait(false);
    }

    /// <summary>
    /// 从DataReader中读取实体列表
    /// </summary>
    private async Task<List<TEntity>> ExecuteReaderAsync<TEntity>(DuckDBCommand command)
    {
        using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        var results = new List<TEntity>();

        var type = typeof(TEntity);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(prop => !Attribute.IsDefined(prop, typeof(NotMappedAttribute)))
            .ToArray();

        // 缓存列索引以提高性能
        var columnIndexMap = new Dictionary<string, int>();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnIndexMap[reader.GetName(i)] = i;
        }

        var columnMappings = new Dictionary<PropertyInfo, int>();

        foreach (var prop in properties)
        {
            var columnName = prop.GetCustomAttribute<ColumnAttribute>()?.Name ?? prop.Name;

            // 查找匹配的列
            if (columnIndexMap.TryGetValue(columnName, out int columnIndex))
            {
                columnMappings[prop] = columnIndex;
            }
        }

        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var entity = Activator.CreateInstance<TEntity>();

            foreach (var mapping in columnMappings)
            {
                var prop = mapping.Key;
                var columnIndex = mapping.Value;

                if (reader.IsDBNull(columnIndex))
                    continue;

                var value = reader.GetValue(columnIndex);

                try
                {
                    // 处理类型转换
                    if (prop.PropertyType.IsEnum && value is string strValue)
                    {
                        // 字符串枚举值转换
                        prop.SetValue(entity, Enum.Parse(prop.PropertyType, strValue));
                    }
                    else if (prop.PropertyType == typeof(DateTime) && value is string dateStr)
                    {
                        // 字符串日期转换
                        prop.SetValue(entity, DateTime.Parse(dateStr));
                    }
                    else if (prop.PropertyType == typeof(Guid) && value is string guidStr)
                    {
                        // 字符串GUID转换
                        prop.SetValue(entity, Guid.Parse(guidStr));
                    }
                    else
                    {
                        // 标准类型转换
                        prop.SetValue(entity, Convert.ChangeType(value, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType));
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(
                        $"属性{prop.Name}设值失败。值类型:{value?.GetType().Name ?? "null"}，" +
                        $"目标类型:{prop.PropertyType.Name}，值:{value}，错误:{ex.Message}", ex);
                    // 继续处理下一个属性
                }
            }

            results.Add(entity);
        }

        return results;
    }

    #endregion

    #region IDisposable Implementation

    /// <summary>
    /// 实现 IDisposable 接口，释放资源。
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 释放托管和非托管资源。
    /// </summary>
    /// <param name="disposing">是否释放托管资源。</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                try
                {
                    // 首先回滚所有未完成的事务
                    List<DuckDBTransaction> transactions;
                    lock (_transactionsLock)
                    {
                        transactions = new List<DuckDBTransaction>(_activeTransactions);
                    }

                    foreach (var transaction in transactions)
                    {
                        try
                        {
                            transaction.Rollback();
                            transaction.Dispose();
                        }
                        catch (Exception ex)
                        {
                            _logger.Error($"回滚事务时出错: {ex.Message}", ex);
                        }
                    }

                    lock (_transactionsLock)
                    {
                        _activeTransactions.Clear();
                    }

                    // 关闭和释放连接
                    if (_connection != null)
                    {
                        if (_connection.State == System.Data.ConnectionState.Open)
                        {
                            try
                            {
                                _connection.Close();
                            }
                            catch (Exception ex)
                            {
                                _logger.Error($"关闭DuckDB连接时出错: {ex.Message}", ex);
                            }
                        }

                        _connection.Dispose();
                        _connection = null;
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error($"释放DuckDB连接资源时出错: {ex.Message}", ex);
                }
            }

            _disposed = true;
        }
    }

    #endregion
}
