using GrpcLeaseService;
using Microsoft.Net.Http.Headers;
using TransactionManager.Services;
using Grpc.Net.Client;
using System.Net;

static List<(int, string)> FromStringToNodes(string urls)
{
    urls = urls.Trim('[');
    urls = urls.Trim(']');

    List<(int, string)> nodes = new List<(int, string)>();
    string[] leaseManagersUrlsAux = urls.Split(',');
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

// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

// Add services to the container.
builder.Services.AddGrpc();

int nodeId = int.Parse(args[0]);
string transactionManagersUrls = args[1];

/*
List<(int, string)> transactionManagerServers = new List<(int, string)> {
    (5001, "http://localhost:5001"),
    (5002, "http://localhost:5002"),
};
*/
List<(int, string)> transactionManagerServers = FromStringToNodes(transactionManagersUrls);

DadTkvService dadTkvService = new DadTkvService(GetChannels(transactionManagerServers), transactionManagerServers[nodeId].Item2);

string ip = Dns.GetHostEntry("localhost").AddressList[0].ToString();
builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Parse(ip), transactionManagerServers[nodeId].Item1);
});

LeaseManagerService leaseManagerService = new LeaseManagerService(dadTkvService);


builder.Services.AddGrpc();
builder.Services.AddSingleton<DadTkvService>(dadTkvService);
builder.Services.AddSingleton<LeaseManagerService>(leaseManagerService);

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<DadTkvService>();
app.MapGrpcService<LeaseManagerService>();
/*
using var channel = GrpcChannel.ForAddress("http://localhost:7001");
var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
RequestLeaseRequest request = new RequestLeaseRequest();
request.TransactionManager = "localhost:7001";
client.RequestLease(request);
*/

app.Run();
