using Abp.Console.Configuration;
using Abp.Console.Serilog;
using Abp.Dependency;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Abp.DuckDB.Examples;

public class Program
{
    public static void Main(string[] args)
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        var app = CreateHostBuilder(args).Build();

        app.Run();
    }

    private static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .UseWindowsService() // Sets the host lifetime to WindowsServiceLifetime...
            .ConfigureServices((hostingContext, services) =>
            {
                var bootstrapper = AbpBootstrapper.Create<AuditingExamplesModule>();

                services.AddSingleton(bootstrapper);

                var configuration = hostingContext.HostingEnvironment.GetAppConfiguration();

                //Configure Serilog logging
                var logger = SerilogConfigurer.Configure(bootstrapper.IocManager.IocContainer, configuration);

                //continue...
                bootstrapper.Initialize();

                services.AddHostedService<Worker>();
            })
            .UseCastleWindsor(IocManager.Instance.IocContainer);
}
