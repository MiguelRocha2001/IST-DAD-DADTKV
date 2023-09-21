namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class LeaseManagerService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    public override Task<RequestLeaseReply> RequestLease(RequestLeaseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        Task.Delay(10000).Wait(); // simulates the time it takes to obtain a lease
        return Task.FromResult(new RequestLeaseReply{});
    }
}