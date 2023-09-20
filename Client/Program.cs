using Grpc.Net.Client;
using GrpcDADTKVClient;

// The port number must match the port of the gRPC server.
using var channel = GrpcChannel.ForAddress("http://localhost:5000");

var client = new DADTKV.DADTKVClient(channel);
TxSubmitReply reply = await Task.FromResult(client.TxSubmit(
                  new TxSubmitRequest {  }));

Console.WriteLine("Reply: " + reply.Result);
Console.WriteLine("Press any key to exit...");
Console.ReadKey();
