using domain;
using System.Net;
using Grpc.Net.Client;
using GrpcPaxos;
using LeaseManager.Services;
// using PaxosClient;

static GrpcChannel[] GetChannels(List<(int, string)> nodes)
{
    GrpcChannel[] channels = new GrpcChannel[nodes.Count];
    int i = 0;
    foreach (var node in nodes)
    {
        channels[i] = GrpcChannel.ForAddress(node.Item2);
        i++;
    }
    return channels;
}

var builder = WebApplication.CreateBuilder(args);

List<(int, string)> nodes = new List<(int, string)> // for testing purposes
        {
            (6001, "http://localhost:6001"),
            (6002, "http://localhost:6002")
        };
var nodeId = int.Parse(args[0]);

string ip = Dns.GetHostEntry("localhost").AddressList[0].ToString();
builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Parse(ip), nodes[nodeId].Item1);
});

List<LeaseRequest> requests = new List<LeaseRequest>();
AcceptedValue acceptedValue = new AcceptedValue();

LeaseManagerService leaseManagerService = new LeaseManagerService(nodeId);
PaxosService paxosService = new PaxosService(nodeId, GetChannels(nodes), leaseManagerService);

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


