using Castle.Core.Logging;
using Microsoft.Extensions.Hosting;

namespace Abp.DuckDB.Examples
{
    /// <summary>
    /// .NET Worker Service
    /// https://ittranslator.cn/dotnet/csharp/2021/05/17/worker-service-gracefully-shutdown.html
    /// </summary>
    public class Worker : BackgroundService
    {
        /// <summary>
        /// 状态：0-默认状态，1-正在完成关闭前的必要工作，2-正在执行 StopAsync
        /// </summary>
        private volatile int _status = 0; //状态

        #region ctor

        private readonly IHostApplicationLifetime _hostApplicationLifetime;
        private readonly ILogger _logger;

        public Worker(ILogger logger, IHostApplicationLifetime hostApplicationLifetime)
        {
            _logger = logger;
            _hostApplicationLifetime = hostApplicationLifetime;
        }

        #endregion

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            // 注册应用停止前需要完成的操作
            _hostApplicationLifetime.ApplicationStopping.Register(() => { GetOffWork(); });

            _logger.Info("StartAsync");

            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                // 这里实现实际的业务逻辑
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        _logger.InfoFormat("Worker running at: {0}", DateTimeOffset.Now);

                        _logger.InfoFormat("DuckDB Parquet查询示例");
                        _logger.InfoFormat("----------------------------");

                        // SQL查询
                        string sqlQuery = @"SELECT AccountId, SUM(ResultCount) as Sum 
                              FROM read_parquet(['...']) 
                              WHERE AccountId IN (18,8,10,11) 
                              GROUP BY AccountId 
                              ORDER BY Sum DESC";

                        try
                        {
                            _logger.InfoFormat("连接到DuckDB并执行查询...");

                            // 使用服务类执行查询
                            using var queryService = new DuckDbParquetQueryService();
                            var stats = await queryService.PrintQueryResultsAsync(sqlQuery);

                            _logger.InfoFormat($"查询完成，共返回 {stats.RowCount} 行，耗时: {stats.ExecutionTimeMs}毫秒");
                        }
                        catch (Exception ex)
                        {
                            _logger.InfoFormat($"查询执行过程中发生错误:");
                            _logger.InfoFormat($"错误信息: {ex.Message}");
                            _logger.InfoFormat($"堆栈跟踪: {ex.StackTrace}");
                        }

                        _logger.WarnFormat("Worker stop at: {0}", DateTimeOffset.Now);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex.Message);
                    }

                    await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
                }
            }
            finally
            {
                _logger.Warn("My worker service shut down.");
            }
        }

        /// <summary>
        /// 关闭前需要完成的工作
        /// </summary>
        private void GetOffWork()
        {
            _status = 1;

            _logger.Warn("处理关闭前必须完成的工作");
        }

        /// <summary>
        /// 关闭服务
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _status = 2;

            _logger.Warn("停止服务");

            return base.StopAsync(cancellationToken);
        }
    }
}
