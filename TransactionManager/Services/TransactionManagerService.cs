namespace TransactionManager.Services;

using Grpc.Core;
using GrpcDADTKV;
using Grpc.Net.Client;

public class TransactionManagerService : DADTKV.DADTKVBase
{

    private class Lease
    {
        public HashSet<string> permissions;

        public Lease(HashSet<string> permissions)
        {
            this.permissions = permissions;
        }
    }

    private string nodeUrl = "CHANGE_ME"; // url of this node
    HashSet<string> tmNodes = new HashSet<string>(); // set of all TM nodes (excluding this one)
    HashSet<string> lmNodes = new HashSet<string>(); // set of all LM nodes
    HashSet<DadInt> storage = new HashSet<DadInt>(); // set of all dadInts stored in this node
    HashSet<Lease> leases = new HashSet<Lease>(); // set of all leases stored in this node

    bool CheckForNecessaryLeasesForReadOperations(IEnumerable<string> reads)
    {
        foreach (string read in reads)
        {
            foreach (Lease lease in leases)
            {
                if (lease.permissions.Contains(read)) return false;
            }
        }
        return true;
    }

    bool CheckForNecessaryLeasesForWriteOperations(IEnumerable<DadInt> writes)
    {
        foreach (DadInt write in writes)
        {
            foreach (Lease lease in leases)
            {
                if (lease.permissions.Contains(write.Key)) return false;
            }
        }
        return true;
    }

    bool CheckForNecessaryLeases(IEnumerable<string> reads, IEnumerable<DadInt> writes)
    {
        return CheckForNecessaryLeasesForReadOperations(reads) && 
            CheckForNecessaryLeasesForWriteOperations(writes);
    }

    Lease RequestLease(HashSet<string> requestPermissions)
    {
        HashSet<Task> requests = new HashSet<Task>();
        foreach (string lmNode in lmNodes)
        {
            using var channel = GrpcChannel.ForAddress("http://localhost:" + lmNode);
            var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
            RequestLeaseRequest request = new RequestLeaseRequest();
            request.TransactionManager = nodeUrl;
            request.Permissions.Add(requestPermissions);
            Task<RequestLeaseReply> task = Task.FromResult(client.RequestLease(request)); // this should not blcock. Change not to block
            requests.Add(task);
        }
        Task.WhenAny(requests);
        return new Lease(requestPermissions);
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

    /**
        Checks if a brother node requires a lease
        @return true if a brother node requires a lease, false otherwise
    */
    bool BrotherNodeRequiresLease()
    {
        throw new NotImplementedException();
    }

    /**
        Releases a lease
    */
    void ReleaseLease(Lease lease)
    {
        throw new NotImplementedException();
    }
    
    public override Task<TxSubmitReply> TxSubmit(TxSubmitRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Client: {request.Client}");

        bool hasLeases = CheckForNecessaryLeases(request.Reads, request.Writes);
        TxSubmitReply reply = new TxSubmitReply();
        Lease lease = null;
        if (!hasLeases)
        {
            Console.WriteLine("Leases not available");
            HashSet<string> permissions = request.Writes.Select(x => x.Key).ToHashSet()
                .Union(request.Reads.AsEnumerable()).ToHashSet();
            lease = RequestLease(permissions);
        }
        Console.WriteLine("Leases available");
        DoTransaction(request.Client, request.Reads, request.Writes);
        if (BrotherNodeRequiresLease())
            ReleaseLease(lease);

        return Task.FromResult(reply);
    }

}
