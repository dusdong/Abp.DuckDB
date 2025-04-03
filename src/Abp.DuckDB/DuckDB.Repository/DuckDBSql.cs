using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;

namespace Abp.DuckDB.Repository;

/// <summary>
/// 用于构建 DuckDB 数据库的 SQL 语句，包括插入、更新、删除、查询等操作。
/// </summary>
public class DuckDBSql
{
    private StringBuilder _sqlBuilder;
    private readonly List<DuckDBParameter> _parameters;
    private readonly Regex _identifierPattern = new Regex(@"^[a-zA-Z0-9_]+$", RegexOptions.Compiled);

    public DuckDBSql()
    {
        _sqlBuilder = new StringBuilder();
        _parameters = new List<DuckDBParameter>();
    }

    /// <summary>
    /// 重置SQL构建器，用于重用
    /// </summary>
    public DuckDBSql Reset()
    {
        _sqlBuilder = new StringBuilder();
        _parameters.Clear();
        return this;
    }

    /// <summary>
    /// 追加 SQL 片段。
    /// </summary>
    public DuckDBSql Append(string sqlPart)
    {
        _sqlBuilder.Append(sqlPart);
        return this;
    }

    /// <summary>
    /// 添加参数。
    /// </summary>
    public DuckDBSql AddParameter(object value)
    {
        _parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        return this;
    }

    /// <summary>
    /// 添加带名称的参数。
    /// </summary>
    public DuckDBSql AddNamedParameter(string name, object value)
    {
        _parameters.Add(new DuckDBParameter { ParameterName = name, Value = value ?? DBNull.Value });
        return this;
    }

    /// <summary>
    /// 获取完整的 SQL 语句。
    /// </summary>
    public string GetSql()
    {
        return _sqlBuilder.ToString();
    }

    /// <summary>
    /// 获取参数列表。
    /// </summary>
    public DuckDBParameter[] GetParameters()
    {
        return _parameters.ToArray();
    }

    /// <summary>
    /// 验证标识符（表名、列名）是否安全
    /// </summary>
    private string ValidateIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || !_identifierPattern.IsMatch(name))
            throw new ArgumentException($"无效的SQL标识符: {name}");
        return name;
    }

    #region Insert Methods

    /// <summary>
    /// 构建插入单个实体的 SQL 语句。
    /// </summary>
    public DuckDBSql Insert<TEntity>(TEntity entity)
    {
        // 获取表名
        string tableName = ValidateIdentifier(GetTableName<TEntity>());

        // 获取需要映射的属性，排除自增的主键列
        var properties = GetProperties<TEntity>(prop =>
        {
            var columnAttribute = prop.GetCustomAttribute<ColumnAttribute>();
            bool isAutoIncrement = columnAttribute?.IsAutoIncrement ?? false;
            return !isAutoIncrement;
        });

        var columnNames = new List<string>();
        var paramPlaceholders = new List<string>();

        foreach (var prop in properties)
        {
            string columnName = ValidateIdentifier(
                prop.GetCustomAttribute<ColumnAttribute>()?.Name ?? prop.Name);

            string paramName = $"p{_parameters.Count + 1}";
            columnNames.Add(columnName);
            paramPlaceholders.Add($"@{paramName}");

            var value = prop.GetValue(entity);
            AddNamedParameter(paramName, value);
        }

        string columns = string.Join(", ", columnNames);
        string placeholders = string.Join(", ", paramPlaceholders);

        _sqlBuilder.Append($"INSERT INTO {tableName} ({columns}) VALUES ({placeholders});");

        return this;
    }

    /// <summary>
    /// 构建插入多个实体的 SQL 语句（批量插入）。
    /// </summary>
    public DuckDBSql Insert<TEntity>(IEnumerable<TEntity> entities, int batchSize = 500)
    {
        var entitiesList = entities.ToList();
        if (entitiesList.Count == 0)
            return this;

        // 获取表名
        string tableName = ValidateIdentifier(GetTableName<TEntity>());

        // 获取需要映射的属性，排除自增的主键列
        var properties = GetProperties<TEntity>(prop =>
        {
            var columnAttribute = prop.GetCustomAttribute<ColumnAttribute>();
            bool isAutoIncrement = columnAttribute?.IsAutoIncrement ?? false;
            return !isAutoIncrement;
        });

        var columnNames = properties
            .Select(prop => ValidateIdentifier(
                prop.GetCustomAttribute<ColumnAttribute>()?.Name ?? prop.Name))
            .ToList();

        string columns = string.Join(", ", columnNames);

        // 分批处理，避免参数过多
        for (int batchIndex = 0; batchIndex < entitiesList.Count; batchIndex += batchSize)
        {
            if (batchIndex > 0)
                _sqlBuilder.Append(";");

            var batch = entitiesList
                .Skip(batchIndex)
                .Take(batchSize)
                .ToList();

            var batchValues = new List<string>();

            foreach (var entity in batch)
            {
                var paramPlaceholders = new List<string>();

                foreach (var prop in properties)
                {
                    string paramName = $"p{_parameters.Count + 1}";
                    paramPlaceholders.Add($"@{paramName}");

                    var value = prop.GetValue(entity);
                    AddNamedParameter(paramName, value);
                }

                batchValues.Add($"({string.Join(", ", paramPlaceholders)})");
            }

            _sqlBuilder.Append($"INSERT INTO {tableName} ({columns}) VALUES {string.Join(", ", batchValues)}");
        }

        return this;
    }

    #endregion

    #region Update Methods

    /// <summary>
    /// 构建更新实体的 SQL 语句。
    /// </summary>
    public DuckDBSql Update<TEntity>(TEntity entity, Expression<Func<TEntity, bool>> predicate)
    {
        // 获取表名
        string tableName = ValidateIdentifier(GetTableName<TEntity>());

        // 获取需要映射的属性
        var properties = GetProperties<TEntity>(prop =>
        {
            var columnAttribute = prop.GetCustomAttribute<ColumnAttribute>();
            bool isAutoIncrement = columnAttribute?.IsAutoIncrement ?? false;
            bool isPrimaryKey = columnAttribute?.IsPrimaryKey ?? false;
            return !isAutoIncrement && !isPrimaryKey; // 排除自增主键
        });

        var setClauses = new List<string>();
        foreach (var prop in properties)
        {
            string columnName = ValidateIdentifier(
                prop.GetCustomAttribute<ColumnAttribute>()?.Name ?? prop.Name);

            string paramName = $"p{_parameters.Count + 1}";
            setClauses.Add($"{columnName} = @{paramName}");

            var value = prop.GetValue(entity);
            AddNamedParameter(paramName, value);
        }

        string setClause = string.Join(", ", setClauses);
        _sqlBuilder.Append($"UPDATE {tableName} SET {setClause}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    #endregion

    #region Delete Methods

    /// <summary>
    /// 构建删除实体的 SQL 语句。
    /// </summary>
    public DuckDBSql Delete<TEntity>(Expression<Func<TEntity, bool>> predicate)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        _sqlBuilder.Append($"DELETE FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    #endregion

    #region Select Methods

    /// <summary>
    /// 构建查询实体的 SQL 语句。
    /// </summary>
    public DuckDBSql Select<TEntity>(Expression<Func<TEntity, bool>> predicate = null)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        _sqlBuilder.Append($"SELECT * FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        return this;
    }

    /// <summary>
    /// 直接指定WHERE子句
    /// </summary>
    public DuckDBSql WhereRaw(string whereClause)
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
        {
            _sqlBuilder.Append(" WHERE ");
            _sqlBuilder.Append(whereClause);
        }

        return this;
    }

    /// <summary>
    /// 构建从 Parquet 文件查询的 SQL 语句。
    /// </summary>
    public DuckDBSql SelectFromParquet<TEntity>(IEnumerable<string> parquetFilePaths, Expression<Func<TEntity, bool>> predicate = null)
    {
        var filePaths = parquetFilePaths.ToList();
        if (filePaths.Count == 0)
            throw new ArgumentException("必须提供至少一个Parquet文件路径");

        if (filePaths.Count == 1)
        {
            // 单文件查询
            string paramName = $"filepath_0";
            AddNamedParameter(paramName, filePaths[0]);
            _sqlBuilder.Append($"SELECT * FROM read_parquet(@{paramName})");

            if (predicate != null)
            {
                var whereClause = BuildWhereClause(predicate);
                _sqlBuilder.Append($" WHERE {whereClause}");
            }

            return this;
        }

        // 多文件查询
        var unionQueries = new List<string>();
        int index = 0;

        foreach (var filePath in filePaths)
        {
            string tableAlias = $"t{index}";
            string paramName = $"filepath_{index}";
            AddNamedParameter(paramName, filePath);

            unionQueries.Add($"SELECT * FROM read_parquet(@{paramName}) AS {tableAlias}");
            index++;
        }

        var combinedQuery = string.Join(" UNION ALL ", unionQueries);
        _sqlBuilder.Append($"WITH combined AS ({combinedQuery}) SELECT * FROM combined");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate, "combined");
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        return this;
    }

    #endregion

    #region Count and Sum Methods

    /// <summary>
    /// 构建 COUNT 查询的 SQL 语句。
    /// </summary>
    public DuckDBSql Count<TEntity>(Expression<Func<TEntity, bool>> predicate = null)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        _sqlBuilder.Append($"SELECT COUNT(*) FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    /// <summary>
    /// 构建 SUM 查询的 SQL 语句。
    /// </summary>
    public DuckDBSql Sum<TEntity>(Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        string columnName = GetColumnName(selector);

        _sqlBuilder.Append($"SELECT SUM({columnName}) FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    /// <summary>
    /// 构建 AVG 查询的 SQL 语句。
    /// </summary>
    public DuckDBSql Avg<TEntity>(Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        string columnName = GetColumnName(selector);

        _sqlBuilder.Append($"SELECT AVG({columnName}) FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    /// <summary>
    /// 构建 MIN 查询的 SQL 语句。
    /// </summary>
    public DuckDBSql Min<TEntity>(Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        string columnName = GetColumnName(selector);

        _sqlBuilder.Append($"SELECT MIN({columnName}) FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    /// <summary>
    /// 构建 MAX 查询的 SQL 语句。
    /// </summary>
    public DuckDBSql Max<TEntity>(Expression<Func<TEntity, decimal>> selector, Expression<Func<TEntity, bool>> predicate = null)
    {
        string tableName = ValidateIdentifier(GetTableName<TEntity>());
        string columnName = GetColumnName(selector);

        _sqlBuilder.Append($"SELECT MAX({columnName}) FROM {tableName}");

        if (predicate != null)
        {
            var whereClause = BuildWhereClause(predicate);
            _sqlBuilder.Append($" WHERE {whereClause}");
        }

        _sqlBuilder.Append(";");
        return this;
    }

    #endregion

    #region Order By, Limit, Offset

    /// <summary>
    /// 添加ORDER BY子句
    /// </summary>
    public DuckDBSql OrderBy(string columnName, bool ascending = true)
    {
        _sqlBuilder.Append($" ORDER BY {ValidateIdentifier(columnName)} {(ascending ? "ASC" : "DESC")}");
        return this;
    }

    /// <summary>
    /// 添加ORDER BY子句 (多列)
    /// </summary>
    public DuckDBSql OrderBy(IEnumerable<(string ColumnName, bool Ascending)> orderColumns)
    {
        if (orderColumns?.Any() != true)
            return this;

        var orderClauses = orderColumns
            .Select(col => $"{ValidateIdentifier(col.ColumnName)} {(col.Ascending ? "ASC" : "DESC")}");

        _sqlBuilder.Append(" ORDER BY ");
        _sqlBuilder.Append(string.Join(", ", orderClauses));

        return this;
    }

    /// <summary>
    /// 添加LIMIT子句
    /// </summary>
    public DuckDBSql Limit(int limit)
    {
        if (limit > 0)
            _sqlBuilder.Append($" LIMIT {limit}");
        return this;
    }

    /// <summary>
    /// 添加OFFSET子句
    /// </summary>
    public DuckDBSql Offset(int offset)
    {
        if (offset > 0)
            _sqlBuilder.Append($" OFFSET {offset}");
        return this;
    }

    /// <summary>
    /// 添加分页 (组合LIMIT和OFFSET)
    /// </summary>
    public DuckDBSql Page(int pageIndex, int pageSize)
    {
        if (pageSize <= 0)
            throw new ArgumentException("页大小必须大于0");

        if (pageIndex <= 0)
            throw new ArgumentException("页索引必须大于0");

        return Limit(pageSize).Offset((pageIndex - 1) * pageSize);
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// 获取表名。
    /// </summary>
    private string GetTableName<TEntity>()
    {
        return typeof(TEntity).GetCustomAttribute<TableAttribute>()?.Name ?? typeof(TEntity).Name;
    }

    /// <summary>
    /// 获取需要映射到数据库的属性列表，排除被 [NotMapped] 标记的属性。
    /// </summary>
    private IEnumerable<PropertyInfo> GetProperties<TEntity>(Func<PropertyInfo, bool> predicate = null)
    {
        var properties = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(prop => !Attribute.IsDefined(prop, typeof(NotMappedAttribute)));

        if (predicate != null)
        {
            properties = properties.Where(predicate);
        }

        return properties;
    }

    /// <summary>
    /// 构建 WHERE 子句。
    /// </summary>
    private string BuildWhereClause<TEntity>(Expression<Func<TEntity, bool>> predicate, string tableAlias = null)
    {
        var visitor = new DuckDBExpressionVisitor(this, tableAlias);
        return visitor.Translate(predicate);
    }

    /// <summary>
    /// 获取列名，从选择器表达式中提取属性名称。
    /// </summary>
    private string GetColumnName<TEntity>(Expression<Func<TEntity, decimal>> selector)
    {
        if (selector.Body is MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }

        throw new ArgumentException("Selector 必须是一个属性访问表达式。");
    }

    /// <summary>
    /// 结束SQL语句（添加分号）
    /// </summary>
    public DuckDBSql End()
    {
        if (!_sqlBuilder.ToString().TrimEnd().EndsWith(";"))
            _sqlBuilder.Append(";");
        return this;
    }

    #endregion
}
