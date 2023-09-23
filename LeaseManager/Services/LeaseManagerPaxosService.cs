namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class LeaseManagerPaxosService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        
        // TODO: store this request and delay until next paxos instance is executed
        // maybe use a pool request
        // maybe put this thread to sleep and wake it up when the paxos instance is executed

        return Task.FromResult(new RequestLeaseReply{});
    }

    public override Task<AcceptReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        return Task.FromResult(new RequestLeaseReply{});
    }

    public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        return Task.FromResult(new RequestLeaseReply{});
    }

    public override Task<AcceptReply> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        return Task.FromResult(new RequestLeaseReply{});
    }
}