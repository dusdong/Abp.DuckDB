using Abp.Dependency;
using Abp.DuckDB;
using Abp.Modules;
using Abp.Reflection.Extensions;

namespace Abp;

public class AbpDuckDBModule : AbpModule
{
    public override void PreInitialize()
    {
        IocManager.Register<SqlBuilder, SqlBuilder>(DependencyLifeStyle.Transient);
        
        IocManager.Register<QueryPerformanceMonitor, QueryPerformanceMonitor>(DependencyLifeStyle.Transient);
    }

    public override void Initialize()
    {
        IocManager.RegisterAssemblyByConvention(typeof(AbpDuckDBModule).GetAssembly());
    }
}
