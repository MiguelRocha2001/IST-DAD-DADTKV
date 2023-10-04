using Grpc.Net.Client;
using GrpcPaxos;
using LeaseManager.Services;
// using PaxosClient;

static GrpcChannel[] GetChannels(HashSet<string> nodes)
{
    GrpcChannel[] channels = new GrpcChannel[nodes.Count];
    int i = 0;
    foreach (string node in nodes)
    {
        channels[i] = GrpcChannel.ForAddress(node);
        i++;
    }
    return channels;
}

static int GetNodeId(HashSet<string> nodes, string node)
{
    int i = 0;
    foreach (string n in nodes)
    {
        if (n == node) return i;
        i++;
    }
    return -1;
}

foreach (string arg in args)
{
    Console.WriteLine(arg);
}



var builder = WebApplication.CreateBuilder(args);
// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

HashSet<string> nodes = new HashSet<string> // for testing purposes
        {
            "http://localhost:6001",
            "http://localhost:6002",
            //"http://localhost:6003"
        };

int nodeId = int.Parse(args[0]);
LeaseManagerService leaseManagerService = new LeaseManagerService();
PaxosService paxosService = new PaxosService(/*GetNodeId(nodes, node)*/nodeId, GetChannels(nodes), leaseManagerService);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSingleton<PaxosService>(paxosService);
builder.Services.AddSingleton<LeaseManagerService>(leaseManagerService);

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<LeaseManagerService>();
app.MapGrpcService<PaxosService>();

paxosService.Init();
app.Run();


