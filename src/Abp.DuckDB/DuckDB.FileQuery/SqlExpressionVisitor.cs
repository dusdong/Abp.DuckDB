using System.Linq.Expressions;
using System.Text;

namespace Abp.DuckDB.FileQuery;

/// <summary>
/// 将 C# 表达式树转换为 SQL 条件语句的访问器
/// </summary>
public class SqlExpressionVisitor : ExpressionVisitor
{
    private StringBuilder _sqlBuilder;
    private readonly Dictionary<ExpressionType, string> _operatorMap;

    public SqlExpressionVisitor()
    {
        _sqlBuilder = new StringBuilder();
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
    /// 转换表达式为 SQL 条件字符串
    /// </summary>
    public string Translate<TEntity>(Expression<Func<TEntity, bool>> expression)
    {
        _sqlBuilder.Clear();
        Visit(expression.Body);
        return _sqlBuilder.ToString();
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
            _sqlBuilder.Append($" UNKNOWN_OPERATOR({node.NodeType}) ");
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
            // 处理直接属性访问
            _sqlBuilder.Append(node.Member.Name);
            return node;
        }
        
        // 处理字段和变量访问
        object value = GetMemberValue(node);
        AppendValue(value);
        
        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        AppendValue(node.Value);
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // 处理字符串方法
        if (node.Object?.Type == typeof(string))
        {
            switch (node.Method.Name)
            {
                case "Contains":
                    Visit(node.Object);
                    _sqlBuilder.Append(" LIKE ");
                    var argValue = GetExpressionValue(node.Arguments[0]);
                    _sqlBuilder.Append($"'%{EscapeSqlLike(argValue?.ToString())}%'");
                    return node;
                    
                case "StartsWith":
                    Visit(node.Object);
                    _sqlBuilder.Append(" LIKE ");
                    argValue = GetExpressionValue(node.Arguments[0]);
                    _sqlBuilder.Append($"'{EscapeSqlLike(argValue?.ToString())}%'");
                    return node;
                    
                case "EndsWith":
                    Visit(node.Object);
                    _sqlBuilder.Append(" LIKE ");
                    argValue = GetExpressionValue(node.Arguments[0]);
                    _sqlBuilder.Append($"'%{EscapeSqlLike(argValue?.ToString())}'");
                    return node;
            }
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

    private object GetMemberValue(MemberExpression member)
    {
        // 解析属性或字段访问以获取实际值
        var objectMember = Expression.Convert(member, typeof(object));
        var getterLambda = Expression.Lambda<Func<object>>(objectMember);
        var getter = getterLambda.Compile();
        return getter();
    }
    
    private object GetExpressionValue(Expression expression)
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
    
    private void AppendValue(object value)
    {
        if (value == null)
        {
            _sqlBuilder.Append("NULL");
            return;
        }

        switch (value)
        {
            case string strValue:
                _sqlBuilder.Append($"'{EscapeSql(strValue)}'");
                break;
            case DateTime dateValue:
                _sqlBuilder.Append($"'{dateValue:yyyy-MM-dd HH:mm:ss}'");
                break;
            case bool boolValue:
                _sqlBuilder.Append(boolValue ? "TRUE" : "FALSE");
                break;
            case byte[] bytes:
                _sqlBuilder.Append($"X'{BitConverter.ToString(bytes).Replace("-", "")}'");
                break;
            default:
                _sqlBuilder.Append(Convert.ToString(value).Replace("'", "''"));
                break;
        }
    }

    private string EscapeSql(string input)
    {
        return input?.Replace("'", "''");
    }
    
    private string EscapeSqlLike(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;
        
        // 在 LIKE 模式中转义特殊字符 % _ [
        return EscapeSql(input)
            .Replace("%", "\\%")
            .Replace("_", "\\_")
            .Replace("[", "\\[");
    }
}
