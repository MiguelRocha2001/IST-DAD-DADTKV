namespace TransactionManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class TransactionManagerService : DADTKV.DADTKVBase
{
    HashSet<object> dadInts = new HashSet<object>(); // set of all dadInts stored in this node
    HashSet<object> leases = new HashSet<object>(); // set of all leases stored in this node

    bool CheckForNecessaryLeases(IEnumerable<string> reads, IEnumerable<object> writes)
    {
        throw new NotImplementedException();
    }

    void RequestNecessaryLeases()
    {
        throw new NotImplementedException();
    }

    void DoTransaction(IEnumerable<string> reads, IEnumerable<object> writes)
    {
        throw new NotImplementedException();
    }
    
    public override Task<TxSubmitReply> TxSubmit(TxSubmitRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Client: {request.Client}");

        bool hasLeases = CheckForNecessaryLeases(request.Reads, request.Writes);
        TxSubmitReply reply = new TxSubmitReply();
        if (!hasLeases)
        {
            Console.WriteLine("Leases not available");
            RequestNecessaryLeases();
        }
        else
        {
            Console.WriteLine("Leases available");
            DoTransaction(request.Reads, request.Writes);
        }

        return Task.FromResult(reply);
    }

}
