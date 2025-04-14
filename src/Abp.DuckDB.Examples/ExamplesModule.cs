using System.Reflection;
using Abp.AutoMapper;
using Abp.Console;
using Abp.Console.Configuration;
using Abp.Modules;
using Microsoft.Extensions.Configuration;

namespace Abp.DuckDB.Examples
{
    [DependsOn(
        typeof(AbpConsoleModule),
        typeof(AbpAutoMapperModule),
        typeof(AbpExtensionModule)
    )]
    public class AuditingExamplesModule : AbpModule
    {
        private readonly IConfigurationRoot _appConfiguration;

        public AuditingExamplesModule()
        {
            //读取配置
            _appConfiguration = AppConfigurations.Get();
        }

        public override void PreInitialize()
        {
            Configuration.BackgroundJobs.IsJobExecutionEnabled = false;

            //关闭多租户
            Configuration.Modules.AbpConfiguration.MultiTenancy.IsEnabled = false;            
        }

        public override void Initialize()
        {
            IocManager.RegisterAssemblyByConvention(Assembly.GetExecutingAssembly());
        }

        public override void PostInitialize()
        {
        }
    }
}
