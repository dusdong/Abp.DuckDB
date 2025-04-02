using Abp.Modules;
using Abp.Reflection.Extensions;

namespace Abp;

public class AbpDuckDBModule : AbpModule
{
    public override void Initialize()
    {
        IocManager.RegisterAssemblyByConvention(typeof(AbpDuckDBModule).GetAssembly());
    }
}
