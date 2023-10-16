namespace TransactionManager.Services;

using Grpc.Core;
using GrpcTransactionService;

public class TransactionManagerService : TransactionService.TransactionServiceBase
{
    DadTkvService dadTkvService;

    public TransactionManagerService(DadTkvService dadTkvService)
    {
        this.dadTkvService = dadTkvService;
    }

    public override Task<Empty> ReleaseLease(ReleaseLeaseMessage releaseLeaseMessage, ServerCallContext context)
    {
        Console.WriteLine($"Lease released: {releaseLeaseMessage.Lease}");

        return Task.FromResult(new Empty());
    }

    public override Task<Empty> PropagateTransaction(PropagateTransactionMessage propagateTransactionMessage, ServerCallContext context)
    {
        Console.WriteLine($"Transaction propagation message received: {propagateTransactionMessage.Writes}");

        dadTkvService.ExecuteTransactionLocally(new List<string>(), propagateTransactionMessage.Writes);

        return Task.FromResult(new Empty());
    }
}
