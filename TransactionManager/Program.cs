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

int nodeId = int.Parse(args[0]);

List<(int, string)> transactionManagerServers;
List<(int, string)> leaseManagerServers;
int quorumSize; // same as the number of lease managers

// TODO: this will be used to update the fault tolerance state of the process...
int timeSlots;
string? starts;
int lasts;
List<ProcessState> processState = new List<ProcessState>();

if (args.Length > 1)
{
    transactionManagerServers = Utils.FromStringToNodes(args[1]);
    throw new NotImplementedException("LeaseManagerServers parse not implemented!");
    quorumSize = int.Parse(args[2]);
    timeSlots = int.Parse(args[3]);
    starts = args[4];
    lasts = int.Parse(args[5]);
}
else
{
    transactionManagerServers = new List<(int, string)> {
        (5001, "http://localhost:5001"),
        (5002, "http://localhost:5002"),
    };  
    leaseManagerServers = new List<(int, string)> {
        (6001, "http://localhost:6001"),
        (6002, "http://localhost:6002"),
    };
    quorumSize = 2;
    timeSlots = 10;
    starts = null;
    lasts = 10;
}

if (starts is not null) // used for testing only
{
    DateTime? startTime = Utils.FromStringToDateTime(starts);
    int timeSpan = Utils.GetSecondsApart(startTime.Value, DateTime.Now);
    Console.WriteLine("TM Waiting " + timeSpan + " seconds to start!");
    await Task.Delay(timeSpan * 1000);
    Console.WriteLine("TM Starting!");
}

DadTkvService dadTkvService = new DadTkvService(
    Utils.GetChannels(transactionManagerServers),
    Utils.GetChannels(leaseManagerServers), 
    transactionManagerServers[nodeId].Item2, 
    nodeId
);
LeaseManagerService leaseManagerService = new LeaseManagerService(dadTkvService, quorumSize);
TransactionManagerService transactionManagerService = new TransactionManagerService(dadTkvService);

string ip = Dns.GetHostEntry("localhost").AddressList[0].ToString();
builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Parse(ip), transactionManagerServers[nodeId].Item1);
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
