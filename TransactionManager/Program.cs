using GrpcLeaseService;
using Microsoft.Net.Http.Headers;
using TransactionManager.Services;
using Grpc.Net.Client;

var builder = WebApplication.CreateBuilder(args);

// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682

// Add services to the container.
builder.Services.AddGrpc();

DadTkvService dadTkvService = new DadTkvService(new GrpcChannel[] 
{ 
    GrpcChannel.ForAddress("http://localhost:6001"),
    GrpcChannel.ForAddress("http://localhost:6002") 
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
