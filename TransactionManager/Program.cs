using GrpcLeaseService;
using Microsoft.Net.Http.Headers;
using TransactionManager.Services;
using Grpc.Net.Client;
using System.Net;
using utils;

var builder = WebApplication.CreateBuilder(args);

// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

// Add services to the container.
builder.Services.AddGrpc();

var nodeId = args[0];
string host = args[1];
int port = int.Parse(args[2]);
string[] transactionManagerServersIds = args[3].Split(',');
var transactionManagerServersUrls = args[4].Split(',');
var leaseManagerServersUrls = args[5].Split(',');
int timeSlots = int.Parse(args[6]);
string? starts = args[7] == "null" ? null : args[6];
int lasts = int.Parse(args[8]);

int nodeIndex = transactionManagerServersIds.ToList().IndexOf(nodeId);
string nodeUrl = transactionManagerServersUrls[nodeIndex];

int quorumSize = leaseManagerServersUrls.Count() / 2 + 1;

if (starts is not null) // used for testing only
{
    DateTime? startTime = Utils.FromStringToDateTime(starts);
    int timeSpan = Utils.GetSecondsApart(startTime.Value, DateTime.Now);
    Console.WriteLine("TM Waiting " + timeSpan + " seconds to start!");
    await Task.Delay(timeSpan * 1000);
    Console.WriteLine("TM Starting!");
}

DadTkvService dadTkvService = new DadTkvService(
    Utils.GetChannels(transactionManagerServersUrls),
    Utils.GetChannels(leaseManagerServersUrls),
    transactionManagerServersIds,
    nodeId,
    nodeUrl
);

LeaseManagerService leaseManagerService = new LeaseManagerService(dadTkvService, quorumSize);
TransactionManagerService transactionManagerService = new TransactionManagerService(dadTkvService);

builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Parse(host), port);
});


builder.Services.AddGrpc();
builder.Services.AddSingleton<DadTkvService>(dadTkvService);
builder.Services.AddSingleton<LeaseManagerService>(leaseManagerService);
builder.Services.AddSingleton<TransactionManagerService>(transactionManagerService);

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<DadTkvService>();
app.MapGrpcService<LeaseManagerService>();
app.MapGrpcService<TransactionManagerService>();

app.Run();
