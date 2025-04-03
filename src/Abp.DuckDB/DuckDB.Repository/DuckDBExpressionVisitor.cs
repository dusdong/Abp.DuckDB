using System.Linq.Expressions;
using System.Text;

namespace Abp.DuckDB.Repository;

/// <summary>
/// 表达式访问器，用于将 LINQ 表达式转换为 SQL WHERE 子句。
/// </summary>
public class DuckDBExpressionVisitor : ExpressionVisitor
{
    private readonly DuckDBSql _sqlBuilder;
    private readonly string _tableAlias;
    private readonly StringBuilder _whereClause;
    private int _paramCounter = 0;

    public DuckDBExpressionVisitor(DuckDBSql sqlBuilder, string tableAlias = null)
    {
        _sqlBuilder = sqlBuilder;
        _tableAlias = tableAlias;
        _whereClause = new StringBuilder();
    }

    public string Translate(Expression expression)
    {
        Visit(expression);
        return _whereClause.ToString();
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        _whereClause.Append("(");
        Visit(node.Left);
        _whereClause.Append($" {GetSqlOperator(node.NodeType)} ");
        Visit(node.Right);
        _whereClause.Append(")");
        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // 处理特殊情况 - DateTime属性
        if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter)
        {
            var columnName = GetColumnName(node.Member.Name);

            if (_tableAlias != null)
                _whereClause.Append($"{_tableAlias}.{columnName}");
            else
                _whereClause.Append(columnName);
        }
        else
        {
            // 处理属性值作为常量的情况
            var value = Expression.Lambda(node).Compile().DynamicInvoke();
            string paramName = $"p{++_paramCounter}";
            _whereClause.Append($"@{paramName}");
            _sqlBuilder.AddNamedParameter(paramName, value);
        }

        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        string paramName = $"p{++_paramCounter}";
        _whereClause.Append($"@{paramName}");
        _sqlBuilder.AddNamedParameter(paramName, node.Value);
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.Name == "Contains" && node.Object?.Type == typeof(string))
        {
            // 字符串的Contains方法
            Visit(node.Object);
            _whereClause.Append(" LIKE CONCAT('%', ");
            Visit(node.Arguments[0]);
            _whereClause.Append(", '%')");
            return node;
        }
        else if (node.Method.Name == "Contains" && node.Method.DeclaringType?.IsGenericType == true &&
                 (node.Method.DeclaringType.GetGenericTypeDefinition() == typeof(List<>) ||
                  node.Method.DeclaringType.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
        {
            // 集合的Contains方法
            Visit(node.Arguments[0]);
            _whereClause.Append(" IN (");

            // 获取集合值
            var collection = Expression.Lambda(node.Object).Compile().DynamicInvoke() as System.Collections.IEnumerable;
            bool isFirst = true;

            foreach (var item in collection)
            {
                if (!isFirst) _whereClause.Append(", ");
                string paramName = $"p{++_paramCounter}";
                _whereClause.Append($"@{paramName}");
                _sqlBuilder.AddNamedParameter(paramName, item);
                isFirst = false;
            }

            _whereClause.Append(")");
            return node;
        }
        else if (node.Method.Name == "StartsWith" && node.Object?.Type == typeof(string))
        {
            // 处理StartsWith
            Visit(node.Object);
            _whereClause.Append(" LIKE CONCAT(");
            Visit(node.Arguments[0]);
            _whereClause.Append(", '%')");
            return node;
        }
        else if (node.Method.Name == "EndsWith" && node.Object?.Type == typeof(string))
        {
            // 处理EndsWith
            Visit(node.Object);
            _whereClause.Append(" LIKE CONCAT('%', ");
            Visit(node.Arguments[0]);
            _whereClause.Append(")");
            return node;
        }
        else if (node.Method.Name == "ToString")
        {
            // 处理ToString调用
            Visit(node.Object);
            return node;
        }

        return base.VisitMethodCall(node);
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        if (node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
        {
            // 简单类型转换，直接访问操作数
            return Visit(node.Operand);
        }
        else if (node.NodeType == ExpressionType.Not)
        {
            _whereClause.Append("NOT ");
            Visit(node.Operand);
            return node;
        }

        return base.VisitUnary(node);
    }

    private string GetSqlOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"不支持的操作符: {nodeType}")
        };
    }

    private string GetColumnName(string propertyName)
    {
        // 这里可以添加属性名和列名的映射逻辑
        // 例如将驼峰命名的属性转换为下划线分隔的列名
        return propertyName;
    }
}
