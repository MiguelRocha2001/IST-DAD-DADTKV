using Grpc.Net.Client;
using GrpcDADTKVLease;

using var channel = GrpcChannel.ForAddress("http://localhost:6002");
var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
RequestLeaseRequest request = new RequestLeaseRequest();
request.TransactionManager = "localhost:5000";
client.RequestLease(request);