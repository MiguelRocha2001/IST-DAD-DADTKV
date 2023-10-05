using Grpc.Net.Client;
using GrpcDADTKVLease;

using var channel = GrpcChannel.ForAddress("http://localhost:6001");
var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
RequestLeaseRequest request = new RequestLeaseRequest();
request.TransactionManager = "localhost:5000";
request.Permissions.Add("read");
client.RequestLease(request);
Console.WriteLine("Done");