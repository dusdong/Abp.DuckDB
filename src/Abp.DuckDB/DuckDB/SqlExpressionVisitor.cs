﻿using System.Linq.Expressions;
using System.Text;
using Castle.Core.Logging;

namespace Abp.DuckDB;

/// <summary>
/// 将 C# 表达式树转换为 DuckDB SQL 条件语句的访问器
/// </summary>
public class SqlExpressionVisitor : ExpressionVisitor
{
    private StringBuilder _sqlBuilder;
    private readonly Dictionary<ExpressionType, string> _operatorMap;
    private readonly ILogger _logger;

    public SqlExpressionVisitor(ILogger logger = null)
    {
        _sqlBuilder = new StringBuilder();
        _logger = logger ?? NullLogger.Instance;
        _operatorMap = new Dictionary<ExpressionType, string>
        {
            { ExpressionType.Equal, " = " },
            { ExpressionType.NotEqual, " <> " },
            { ExpressionType.GreaterThan, " > " },
            { ExpressionType.GreaterThanOrEqual, " >= " },
            { ExpressionType.LessThan, " < " },
            { ExpressionType.LessThanOrEqual, " <= " },
            { ExpressionType.AndAlso, " AND " },
            { ExpressionType.OrElse, " OR " },
            { ExpressionType.Add, " + " },
            { ExpressionType.Subtract, " - " },
            { ExpressionType.Multiply, " * " },
            { ExpressionType.Divide, " / " }
        };
    }

    /// <summary>
    /// 转换表达式为 SQL 条件字符串，使用缓存提高性能
    /// </summary>
    public string Translate<TEntity>(Expression<Func<TEntity, bool>> expression)
    {
        if (expression == null)
            return string.Empty;

        // 使用改进的方法生成唯一键
        string expressionKey = GenerateUniqueExpressionKey(expression);

        // 从缓存获取或生成SQL表达式
        string sql = MetadataCache.GetOrAddExpressionSql(expressionKey, _ =>
        {
            _sqlBuilder.Clear();
            Visit(expression.Body);
            string generatedSql = _sqlBuilder.ToString();
            _logger.Debug($"生成SQL: {generatedSql} (表达式: {expression})");
            return generatedSql;
        });

        return sql;
    }

    /// <summary>
    /// 生成唯一的表达式缓存键 - 改进版本
    /// </summary>
    private string GenerateUniqueExpressionKey<TEntity>(Expression<Func<TEntity, bool>> expression)
    {
        // 基本信息
        var keyBuilder = new StringBuilder();
        keyBuilder.Append(typeof(TEntity).FullName);
        keyBuilder.Append("_");

        // 提取表达式中的常量值和成员值
        var constantExtractor = new ConstantExtractor();
        constantExtractor.Visit(expression);

        // 添加表达式结构信息
        keyBuilder.Append(ExpressionToString(expression.Body));
        keyBuilder.Append("_");

        // 添加所有常量值到键中
        int constIndex = 0;
        foreach (var constant in constantExtractor.Constants)
        {
            keyBuilder.Append("C");
            keyBuilder.Append(constIndex++);
            keyBuilder.Append("=");
            keyBuilder.Append(constant?.ToString() ?? "null");
            keyBuilder.Append("_");
        }

        return keyBuilder.ToString();
    }

    /// <summary>
    /// 将表达式转换为描述其结构的字符串（不包含具体值）
    /// </summary>
    private string ExpressionToString(Expression expression)
    {
        if (expression is BinaryExpression binary)
        {
            return $"{ExpressionToString(binary.Left)}{binary.NodeType}{ExpressionToString(binary.Right)}";
        }
        else if (expression is MemberExpression member)
        {
            return member.Member.Name;
        }
        else if (expression is ConstantExpression)
        {
            return "Const";
        }
        else if (expression is UnaryExpression unary)
        {
            return $"{unary.NodeType}{ExpressionToString(unary.Operand)}";
        }
        else if (expression is MethodCallExpression method)
        {
            return $"{method.Method.Name}()";
        }

        return expression.NodeType.ToString();
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        _sqlBuilder.Append("(");
        Visit(node.Left);

        if (_operatorMap.TryGetValue(node.NodeType, out string sqlOperator))
        {
            _sqlBuilder.Append(sqlOperator);
        }
        else
        {
            throw new NotSupportedException($"不支持的二元运算符: {node.NodeType}");
        }

        Visit(node.Right);
        _sqlBuilder.Append(")");

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // 处理属性访问
        if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter)
        {
            // 处理直接属性访问，例如 p.Name
            _sqlBuilder.Append(node.Member.Name);
            return node;
        }

        // 处理字段和变量访问，例如 closure.field
        object value = GetMemberValue(node);
        _sqlBuilder.Append(FormatValue(value));

        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        _sqlBuilder.Append(FormatValue(node.Value));
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // 处理字符串方法
        if (node.Object?.Type == typeof(string))
        {
            HandleStringMethodCall(node);
            return node;
        }

        // 处理其他方法调用
        throw new NotSupportedException($"不支持的方法调用: {node.Method.Name}");
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        if (node.NodeType == ExpressionType.Not)
        {
            _sqlBuilder.Append("NOT ");
            Visit(node.Operand);
            return node;
        }

        if (node.NodeType == ExpressionType.Convert)
        {
            Visit(node.Operand);
            return node;
        }

        return base.VisitUnary(node);
    }

    /// <summary>
    /// 处理字符串相关的方法调用
    /// </summary>
    private void HandleStringMethodCall(MethodCallExpression node)
    {
        // 优化：根据DuckDB支持的函数和模式，选择最高效的实现
        switch (node.Method.Name)
        {
            case "Contains":
                Visit(node.Object); // 字符串对象
                _sqlBuilder.Append(" LIKE ");
                var argValue = GetExpressionValue(node.Arguments[0]);
                // 使用DuckDB的ESCAPE语法保证安全
                _sqlBuilder.Append($"'%{EscapeSqlLike(argValue?.ToString())}%' ESCAPE '\\'");
                break;

            case "StartsWith":
                Visit(node.Object); // 字符串对象
                _sqlBuilder.Append(" LIKE ");
                argValue = GetExpressionValue(node.Arguments[0]);
                _sqlBuilder.Append($"'{EscapeSqlLike(argValue?.ToString())}%' ESCAPE '\\'");
                break;

            case "EndsWith":
                Visit(node.Object); // 字符串对象
                _sqlBuilder.Append(" LIKE ");
                argValue = GetExpressionValue(node.Arguments[0]);
                _sqlBuilder.Append($"'%{EscapeSqlLike(argValue?.ToString())}' ESCAPE '\\'");
                break;

            case "ToUpper":
                _sqlBuilder.Append("UPPER(");
                Visit(node.Object);
                _sqlBuilder.Append(")");
                break;

            case "ToLower":
                _sqlBuilder.Append("LOWER(");
                Visit(node.Object);
                _sqlBuilder.Append(")");
                break;

            case "Trim":
                _sqlBuilder.Append("TRIM(");
                Visit(node.Object);
                _sqlBuilder.Append(")");
                break;

            case "Substring":
                _sqlBuilder.Append("SUBSTRING(");
                Visit(node.Object);
                _sqlBuilder.Append(", ");

                // 获取起始位置参数 (注意DuckDB的索引是从1开始)
                var startIndex = GetExpressionValue(node.Arguments[0]);
                int start = Convert.ToInt32(startIndex) + 1; // C# 是从0开始，SQL是从1开始
                _sqlBuilder.Append(start);

                // 如果有长度参数
                if (node.Arguments.Count > 1)
                {
                    _sqlBuilder.Append(", ");
                    Visit(node.Arguments[1]);
                }

                _sqlBuilder.Append(")");
                break;

            default:
                throw new NotSupportedException($"不支持的字符串方法: {node.Method.Name}");
        }
    }

    /// <summary>
    /// 获取成员表达式的值
    /// </summary>
    private object GetMemberValue(MemberExpression member)
    {
        try
        {
            // 处理嵌套成员访问，如 closure.field.Property
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            var value = getter();
            return value;
        }
        catch (Exception ex)
        {
            _logger.Warn($"获取成员值失败: {member.Member.Name}, 错误: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 获取表达式的值
    /// </summary>
    private object GetExpressionValue(Expression expression)
    {
        try
        {
            if (expression is ConstantExpression constExpr)
            {
                return constExpr.Value;
            }

            if (expression is MemberExpression memberExpr)
            {
                return GetMemberValue(memberExpr);
            }

            var objectMember = Expression.Convert(expression, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }
        catch (Exception ex)
        {
            _logger.Warn($"获取表达式值失败: {expression}, 错误: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 格式化SQL值，针对DuckDB优化
    /// </summary>
    private string FormatValue(object value)
    {
        if (value == null)
        {
            return "NULL";
        }

        switch (value)
        {
            case string strValue:
                return $"'{EscapeSql(strValue)}'";
            case DateTime dateValue:
                return $"TIMESTAMP '{dateValue:yyyy-MM-dd HH:mm:ss.fff}'";
            case DateOnly dateOnlyValue:
                return $"DATE '{dateOnlyValue:yyyy-MM-dd}'";
            case TimeOnly timeOnlyValue:
                return $"TIME '{timeOnlyValue:HH:mm:ss.fff}'";
            case bool boolValue:
                return boolValue ? "TRUE" : "FALSE";
            case byte[] bytes:
                return $"X'{BitConverter.ToString(bytes).Replace("-", "")}'";
            case Guid guidValue:
                return $"'{guidValue}'";
            case decimal decValue:
                return decValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case float floatValue:
                return floatValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case double doubleValue:
                return doubleValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case int intValue:
                return intValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case long longValue:
                return longValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case Enum enumValue:
                return Convert.ToInt32(enumValue).ToString();
            default:
                return EscapeSql(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    /// <summary>
    /// 转义SQL字符串
    /// </summary>
    private string EscapeSql(string input)
    {
        return input?.Replace("'", "''");
    }

    /// <summary>
    /// 转义LIKE模式中的特殊字符 (DuckDB特定语法)
    /// </summary>
    private string EscapeSqlLike(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        // 在 LIKE 模式中转义特殊字符 % _ \ 
        return EscapeSql(input)
            .Replace("\\", "\\\\") // 首先转义反斜杠自身
            .Replace("%", "\\%") // 然后转义其他特殊字符
            .Replace("_", "\\_");
    }

    /// <summary>
    /// 提取表达式中的常量值
    /// </summary>
    private class ConstantExtractor : ExpressionVisitor
    {
        public List<object> Constants { get; } = new List<object>();

        protected override Expression VisitConstant(ConstantExpression node)
        {
            Constants.Add(node.Value);
            return base.VisitConstant(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ConstantExpression)
            {
                try
                {
                    var objectMember = Expression.Convert(node, typeof(object));
                    var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                    var getter = getterLambda.Compile();
                    Constants.Add(getter());
                }
                catch
                {
                    /* 忽略获取失败的情况 */
                }
            }

            return base.VisitMember(node);
        }
    }
}
