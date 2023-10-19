using domain;
using System.Net;
using Grpc.Net.Client;
using GrpcPaxos;
using LeaseManager.Services;
using utils;


var builder = WebApplication.CreateBuilder(args);
Console.WriteLine(string.Join(" ", args));

if (args.Length == 1)
{
    args = 
        ($"{args[0]} 127.0.0.1 600{int.Parse(args[0])+1} "+
        "http://127.0.0.1:6001,http://127.0.0.1:6002 http://127.0.0.1:5001 "+
        "100 null 15 [0N,1C,4N]").Split(" ");
}

Console.WriteLine(string.Join(' ', args));

var nodeId = int.Parse(args[0]);
string host = args[1];
int port = int.Parse(args[2]);
var leaseManagerServers = args[3].Split(',');
var transactionManagerServers = args[4].Split(',');
int timeSlots = int.Parse(args[5]);
string? starts = args[6] == "null" ? null : args[6];
int lasts = int.Parse(args[7]);
ServerState[] serverState = Utils.FromStringToServerState(args[8]);
/*
var serverState = new ServerState[]
{
    ServerState.Normal, ServerState.Normal, ServerState.Normal, ServerState.Normal,
};
*/
List<HashSet<int>> suspectedNodes = new(){
    new(), new(), new(), new()
};

builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Parse(host), port);
});

List<LeaseRequest> requests = new List<LeaseRequest>();
AcceptedValue acceptedValue = new AcceptedValue();

LeaseManagerService leaseManagerService = new LeaseManagerService(
    nodeId, 
    Utils.GetChannels(transactionManagerServers)
);
PaxosService paxosService = new PaxosService(
    nodeId, 
    Utils.GetChannels(leaseManagerServers), 
    leaseManagerService, 
    timeSlots, 
    lasts,
    serverState,
    suspectedNodes
);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSingleton<PaxosService>(paxosService);
builder.Services.AddSingleton<LeaseManagerService>(leaseManagerService);

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<LeaseManagerService>();
app.MapGrpcService<PaxosService>();

if (starts is not null) // used for testing only
{
    DateTime startTime = Utils.FromStringToDateTime(starts);
    int timeSpan = Utils.GetSecondsApart(startTime, DateTime.Now);
    Console.WriteLine("LM Waiting " + timeSpan + " seconds to start!");
    await Task.Delay(timeSpan * 1000);
    Console.WriteLine("LM Starting!");
}

paxosService.Init();
app.Run();


