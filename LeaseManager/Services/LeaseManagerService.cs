namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class LeaseManagerService : DADTKV.DADTKVBase
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
