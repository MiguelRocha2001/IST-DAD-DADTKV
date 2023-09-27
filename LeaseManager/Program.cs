using Grpc.Net.Client;
using GrpcPaxos;
using LeaseManager.Services;
// using PaxosClient;

foreach (string arg in args)
{
    Console.WriteLine(arg);
}



var builder = WebApplication.CreateBuilder(args);
// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

string node = args[1];
HashSet<string> nodes = new HashSet<string> // for testing purposes
        {
            "http://localhost:6001",
            "http://localhost:6002",
            //"http://localhost:6003"
        };

LeaseManagerService leaseManagerService = new LeaseManagerService();
PaxosService paxosService = new PaxosService(node, nodes, leaseManagerService);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSingleton<PaxosService>(paxosService);
builder.Services.AddSingleton<LeaseManagerService>(leaseManagerService);

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<LeaseManagerService>();
app.MapGrpcService<PaxosService>();

/*
using var channel = GrpcChannel.ForAddress("http://localhost:6001");
var client = new Paxos.PaxosClient(channel);
client.Prepare(new PrepareRequest());
*/

app.Run();


