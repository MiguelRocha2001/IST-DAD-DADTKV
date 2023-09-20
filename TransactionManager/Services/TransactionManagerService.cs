namespace TransactionManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class TransactionManagerService : DADTKV.DADTKVBase
{
    public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.Name}");
        return Task.FromResult(new HelloReply
        {
            Message = "Hello " + request.Name
        });
    }
}
