namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class LeaseManagerService : GrpcDADTKV.DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    public override Task<ObtainLeaseReply> ObtainLease(ObtainLeaseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        Task.Delay(10000).Wait(); // simulates the time it takes to obtain a lease
        return Task.FromResult(new ObtainLeaseReply{});
    }
}