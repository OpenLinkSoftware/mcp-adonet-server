using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using McpNetServer.Tools;


var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();

builder.Logging.AddConsole(options =>
{
    options.LogToStandardErrorThreshold = LogLevel.Trace;
});

builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithTools<VirtuosoTools>();

await builder.Build().RunAsync();
