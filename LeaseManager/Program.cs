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

static DateTime? FromStringToDateTime(string starts)
{
    DateTime now = DateTime.Now;
    string[] split = starts.Split(':');
    if (split.Length != 3)
    {
        return null;
    }
    int hour = int.Parse(split[0]);
    int minute = int.Parse(split[1]);
    int second = int.Parse(split[2]);
    return new DateTime(now.Year, now.Month, now.Day, hour, minute, second);
}

/**
    Returns the seconds between two dates.
    [startTime] shhouls be greater than [now].
    @param startTime The start date.
    @param now The end date.
*/
static int GetSecondsApart(DateTime startTime, DateTime now)
{
    TimeSpan timeSpan = startTime - now;
    int totalSeconds = (int)timeSpan.TotalSeconds;
    if (totalSeconds < 0)
        throw new Exception("startTime is in the past!");
    return totalSeconds;
}

var builder = WebApplication.CreateBuilder(args);

var nodeId = int.Parse(args[0]);


List<(int, string)> leaseManagerServers;
List<(int, string)> transactionManagerServers;
int timeSlots;
string starts;
int lasts;

if (args.Length > 1)
{
    leaseManagerServers = FromStringToNodes(args[1]);
    transactionManagerServers = FromStringToNodes(args[2]);
    timeSlots = int.Parse(args[3]);
    starts = args[4];
    lasts = int.Parse(args[5]);
}
else 
{
    leaseManagerServers = new List<(int, string)> {
        (6001, "http://localhost:6001"),
        (6002, "http://localhost:6002"),
    };

    transactionManagerServers = new List<(int, string)> {
        (5001, "http://localhost:5001"),
        (5002, "http://localhost:5002"),
    };
    timeSlots = 10;
    starts = "null";
    lasts = 10;
}

string ip = Dns.GetHostEntry("localhost").AddressList[0].ToString();
builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Parse(ip), leaseManagerServers[nodeId].Item1);
});

List<LeaseRequest> requests = new List<LeaseRequest>();
AcceptedValue acceptedValue = new AcceptedValue();

LeaseManagerService leaseManagerService = new LeaseManagerService(nodeId, GetChannels(transactionManagerServers));
PaxosService paxosService = new PaxosService(nodeId, GetChannels(leaseManagerServers), leaseManagerService, timeSlots, lasts);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSingleton<PaxosService>(paxosService);
builder.Services.AddSingleton<LeaseManagerService>(leaseManagerService);

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<LeaseManagerService>();
app.MapGrpcService<PaxosService>();

if (starts != "null") // used for testing only
{
    DateTime? startTime = FromStringToDateTime(starts);
    int timeSpan = GetSecondsApart(startTime.Value, DateTime.Now);
    Console.WriteLine("LM Waiting " + timeSpan + " seconds to start!");
    await Task.Delay(timeSpan * 1000);
    Console.WriteLine("LM Starting!");
}

paxosService.Init();
app.Run();


