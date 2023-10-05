namespace TransactionManager.Services;

using Grpc.Core;
using GrpcDADTKV;
using Grpc.Net.Client;
using GrpcLeaseService;

public class DadTkvService : DADTKV.DADTKVBase
{
    public string nodeUrl = "CHANGE_ME"; // url of this node
    HashSet<string> tmNodes = new HashSet<string>(); // set of all TM nodes (excluding this one)
    HashSet<DadInt> storage = new HashSet<DadInt>(); // set of all dadInts stored in this node
    public HashSet<Lease> leases = new HashSet<Lease>(); // set of all leases stored in this node
    GrpcChannel[] nodes; // array of all nodes
    object lockObject = new object();

    public DadTkvService(GrpcChannel[] nodes)
    {
        this.nodes = nodes;
    }

    bool CheckForNecessaryLeasesForReadOperations(IEnumerable<string> reads)
    {
        foreach (string read in reads)
        {
            bool found = false;
            foreach (Lease lease in leases)
            {
                if (!lease.RequestIds.Contains(read)) 
                    continue; // try next available lease
                else
                {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    bool CheckForNecessaryLeasesForWriteOperations(IEnumerable<DadInt> writes)
    {
        foreach (DadInt write in writes)
        {
            bool found = false;
            foreach (Lease lease in leases)
            {
                if (!lease.RequestIds.Contains(write.Key)) 
                    continue; // try next available lease
                else
                {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    bool CheckForNecessaryLeases(IEnumerable<string> reads, IEnumerable<DadInt> writes)
    {
        return CheckForNecessaryLeasesForReadOperations(reads) && 
            CheckForNecessaryLeasesForWriteOperations(writes);
    }

    void RequestLease(HashSet<string> requestPermissions)
    {
        foreach (GrpcChannel channel in nodes)
        {
            var client = new LeaseService.LeaseServiceClient(channel);
            Lease request = new Lease();
            request.TransactionManagerId = nodeUrl;
            request.RequestIds.Add(requestPermissions);
            Task.FromResult(client.RequestLease(request)); // this should not blcock. Change not to block
        }
    }

    /**
        Executes the read operations of a transaction
        @return the result of the read operations
    */
    HashSet<DadInt> ExecuteReadOperations(IEnumerable<string> reads)
    {
        HashSet<DadInt> result = new HashSet<DadInt>();
        foreach (string read in reads)
        {
            foreach (DadInt dadInt in storage)
            {
                if (dadInt.Key == read) result.Add(dadInt);
            }
        }
        return result;
    }

    /**
        Executes the write operations of a transaction
    */
    void ExecuteWriteOperations(IEnumerable<DadInt> writes)
    {
        foreach (DadInt write in writes)
        {
            foreach (DadInt dadInt in storage)
            {
                if (dadInt.Key == write.Key) dadInt.Value = write.Value;
            }
        }
    }

    /**
        Executes the transaction locally, ie. without propagating the changes to the other nodes
        @return the transaction result
    */
    HashSet<DadInt> ExecuteTransactionLocally(IEnumerable<string> reads, IEnumerable<DadInt> writes)
    {
        Console.WriteLine("Executing transaction locally...");
        HashSet<DadInt> result = ExecuteReadOperations(reads);
        ExecuteWriteOperations(writes);
        return result;
    }

    /**
        Propagates the transaction to the other nodes
    */
    void PropagateTransaction(string clientName, IEnumerable<string> reads, IEnumerable<DadInt> writes)
    {
        foreach (string nodeUrl in tmNodes)
        {
            using var channel = GrpcChannel.ForAddress("http://localhost:" + nodeUrl); // grpc channel
            var client = new DADTKV.DADTKVClient(channel); // grpc client
            TxSubmitRequest request = new TxSubmitRequest(); // GRPC request
            request.Client = clientName;
            request.Reads.Add(reads);
            request.Writes.Add(writes);
            client.TxSubmit(request);
        }
    }

    /**
        Executes the transaction and propagates the changes to the other nodes
        @return the transaction result
    */
    HashSet<DadInt> DoTransaction(string client, IEnumerable<string> reads, IEnumerable<DadInt> writes)
    {
        Console.WriteLine("Transaction requested by " + client);
        HashSet<DadInt> result = ExecuteTransactionLocally(reads, writes);
        PropagateTransaction(client, reads, writes);
        Console.WriteLine("Transaction finished");
        return result;
    }
    
    public override Task<TxSubmitReply> TxSubmit(TxSubmitRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Client: {request.Client}");

        do
        {
            Console.WriteLine("Checking for necessary leases");
            bool hasLeases = CheckForNecessaryLeases(request.Reads, request.Writes);
            if (!hasLeases)
            {
                Console.WriteLine("Leases not available");
                HashSet<string> permissions = request.Writes.Select(x => x.Key).ToHashSet()
                    .Union(request.Reads.AsEnumerable()).ToHashSet();
                RequestLease(permissions);
            }
            lock (lockObject) 
            {
                Monitor.Wait(lockObject); // waits for some other thread wake it up when new leases are available
            }
        } while (!CheckForNecessaryLeases(request.Reads, request.Writes));
        
        Console.WriteLine("Leases available");
        
        HashSet<DadInt> result = DoTransaction(request.Client, request.Reads, request.Writes); // execute transaction
        TxSubmitReply reply = new TxSubmitReply();
        reply.Result.Add(result);

        return Task.FromResult(reply);
    }

    public override Task<GrpcDADTKV.Empty> Status(GrpcDADTKV.Empty request, ServerCallContext context)
    {
        Console.WriteLine("Status requested");
        return Task.FromResult(new GrpcDADTKV.Empty());
    }

    public void WakeUpWaitingTransactionRequests()
    {
        lock (lockObject)
        {
            Monitor.PulseAll(lockObject);
        }
    }
}
