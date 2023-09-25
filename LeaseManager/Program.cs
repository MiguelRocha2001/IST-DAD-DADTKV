using Grpc.Net.Client;
using GrpcPaxos;
using LeaseManager.Services;
// using PaxosClient;

var builder = WebApplication.CreateBuilder(args);
// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSingleton<PaxosService>();

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapGrpcService<LeaseManagerService>();
app.MapGrpcService<PaxosService>();

//
using var channel = GrpcChannel.ForAddress("http://localhost:5000");
var client = new Paxos.PaxosClient(channel);

var PrepareRequest = new PrepareRequest();
PrepareRequest.Epoch = 5;
client.Prepare(PrepareRequest);
app.Run();


