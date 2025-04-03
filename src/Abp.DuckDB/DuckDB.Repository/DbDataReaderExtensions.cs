using System.Data.Common;

namespace Abp.DuckDB.Repository;

public static class DbDataReaderExtensions
{
    public static bool HasColumn(this DbDataReader reader, string columnName)
    {
        for (int i = 0; i < reader.FieldCount; i++)
        {
            if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}
