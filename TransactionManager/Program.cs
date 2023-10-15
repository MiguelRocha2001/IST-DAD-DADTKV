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

// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

// Add services to the container.
builder.Services.AddGrpc();

int nodeId = int.Parse(args[0]);

List<(int, string)> transactionManagerServers;

// TODO: this will be used to update the fault tolerance state of the process...
int timeSlots;
string starts;
int lasts;

if (args.Length > 1)
{
    transactionManagerServers = FromStringToNodes(args[1]);
    timeSlots = int.Parse(args[2]);
    starts = args[3];
    lasts = int.Parse(args[4]);
}
else
{
    transactionManagerServers = new List<(int, string)> {
        (5001, "http://localhost:5001"),
        (5002, "http://localhost:5002"),
    };  
    timeSlots = 10;
    starts = "null";
    lasts = 10;
}

/*
Console.WriteLine("TimeSlots: " + timeSlots);
Console.WriteLine("Starts: " + starts);
Console.WriteLine("Lasts: " + lasts);
*/

if (starts != "null") // used for testing only
{
    DateTime? startTime = FromStringToDateTime(starts);
    int timeSpan = GetSecondsApart(startTime.Value, DateTime.Now);
    Console.WriteLine("TM Waiting " + timeSpan + " seconds to start!");
    await Task.Delay(timeSpan * 1000);
    Console.WriteLine("TM Starting!");
}

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
