using domain;
using System.Net;
using Grpc.Net.Client;
using GrpcPaxos;
using LeaseManager.Services;
// using PaxosClient;

static List<(int, string)> FromStringToNodes(string leaseManagerUrls)
{
    leaseManagerUrls = leaseManagerUrls.Trim('[');
    leaseManagerUrls = leaseManagerUrls.Trim(']');

    List<(int, string)> nodes = new List<(int, string)>();
    string[] leaseManagersUrlsAux = leaseManagerUrls.Split(',');
    foreach(string serverUlr in leaseManagersUrlsAux )
    {
        string[] split = serverUlr.Split(':');
        nodes.Add((int.Parse(split[2]), serverUlr));
    }
    return nodes;
}

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

var nodeId = int.Parse(args[0]);
List<(int, string)> nodes = FromStringToNodes(args[1]);

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


