namespace TransactionManager.Services;

using Grpc.Core;
using GrpcDADTKV;
using Grpc.Net.Client;
using GrpcLeaseService;
using GrpcTransactionService;

public class DadTkvService : DADTKV.DADTKVBase
{
    public string nodeUrl;
    public int nodeId;
    HashSet<DadInt> storage = new HashSet<DadInt>(); // set of all dadInts stored in this node
    public HashSet<Tuple<bool, Lease>> leases = new HashSet<Tuple<bool, Lease>>(); // set of all leases
    private HashSet<Tuple<Lease, int>> requestedLeases = new HashSet<Tuple<Lease, int>>(); // set of all requested leases
    GrpcChannel[] transactionManagerNodes; // array of all nodes
    GrpcChannel[] leaseManagerNodes; // array of all nodes
    object lockObject = new object();

    public DadTkvService(
        GrpcChannel[] transactionManagerNodes,
        GrpcChannel[] leaseManagerNodes,
        string[] transactionManagerIds,
        string nodeId,
        string nodeUrl
    )
    {
        this.transactionManagerNodes = transactionManagerNodes;
        this.leaseManagerNodes = leaseManagerNodes;
        this.nodeUrl = nodeUrl;
        this.nodeId = nodeId;
    }
    
    bool CheckForNecessaryLeasesForReadOperations(IEnumerable<string> reads)
    {
        foreach (string read in reads)
        {
            bool found = false;
            foreach (Tuple<bool, Lease> tuple in leases)
            {
                if (!tuple.Item2.RequestIds.Contains(read)) 
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
            foreach (Tuple<bool, Lease> tuple in leases)
            {
                if (!tuple.Item2.RequestIds.Contains(write.Key)) 
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

    void RequestLease(Lease lease)
    {
        foreach (GrpcChannel channel in leaseManagerNodes)
        {
            var client = new LeaseService.LeaseServiceClient(channel);
            Task.FromResult(client.RequestLease(lease)); // this should not blcock. Change not to block
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
    public HashSet<DadInt> ExecuteTransactionLocally(IEnumerable<string> reads, IEnumerable<DadInt> writes)
    {
        Console.WriteLine("Executing transaction locally...");
        HashSet<DadInt> result = ExecuteReadOperations(reads);
        ExecuteWriteOperations(writes);
        Console.WriteLine("Transaction executed locally");
        return result;
    }

    /**
        Propagates the transaction to the other nodes
    */
    void PropagateTransaction(IEnumerable<DadInt> writes)
    {
        foreach (GrpcChannel channel in transactionManagerNodes)
        {
            var client = new TransactionService.TransactionServiceClient(channel);
            PropagateTransactionMessage propagateMessage = new PropagateTransactionMessage();
            propagateMessage.Writes.Add(writes);
            client.PropagateTransaction(propagateMessage);
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
        if (writes.ToList().Count > 0) 
        {
            PropagateTransaction(writes);
            Console.WriteLine("Transaction propagated");
        }
        else 
        {
            Console.WriteLine("No need to propagate transaction");
        }
        Console.WriteLine("Transaction finished");
        return result;
    }
    
    public override Task<TxSubmitReply> TxSubmit(TxSubmitRequest request, ServerCallContext context)
    {
        void ReleaseLeaseIfNecessary(Lease lease)
        {
            foreach (Tuple<bool, Lease> leaseTuple in leases)
            {
                if (leaseTuple.Item2.Equals(lease))
                {
                    if (leaseTuple.Item1) // release lease after transaction
                    {
                        leases.Remove(leaseTuple);
                        foreach (GrpcChannel channel in transactionManagerNodes)
                        {
                            if (channel.Target == nodeUrl)
                                break;
                            var client = new TransactionService.TransactionServiceClient(channel);
                            ReleaseLeaseMessage releaseLeaseMessage = new ReleaseLeaseMessage();
                            releaseLeaseMessage.Lease = lease;
                            client.ReleaseLease(releaseLeaseMessage);
                        }
                        Console.WriteLine("Lease with permissions " + lease.RequestIds + " released");
                        return;
                    }
                    else // keep lease
                    {
                        Console.WriteLine("Lease with permissions " + lease.RequestIds + " kept");
                        return;
                    }
                }
            }
        }

        Lease BuildLease(HashSet<string> requestIds)
        {
            Lease lease = new Lease();
            lease.TransactionManagerId = nodeId.ToString();
            lease.RequestIds.Add(requestIds);
            return lease;
        }

        void DecreaseLeaseRequestCount(Lease lease)
        {
            foreach (Tuple<Lease, int> requestedLease in requestedLeases)
            {
                if (requestedLease.Item1.Equals(lease))
                {
                    int count = requestedLease.Item2;
                    if (count == 1) // this is the last request for this lease
                    {
                        requestedLeases.Remove(requestedLease);
                        ReleaseLeaseIfNecessary(lease);
                    }
                    else
                    {
                        requestedLeases.Remove(requestedLease);
                        requestedLeases.Add(new Tuple<Lease, int>(lease, count - 1)); // decreases the number of requests for this lease
                    }
                    break;
                }
            }
        }

        void CheckIfRequestWasAlreadyMade(Lease lease)
        {
            foreach (Tuple<Lease, int> requestedLease in requestedLeases)
            {
                if (requestedLease.Item1.Equals(lease))
                {
                    int count = requestedLease.Item2;
                    requestedLeases.Remove(requestedLease);
                    requestedLeases.Add(new Tuple<Lease, int>(lease, count + 1)); // increases the number of requests for this lease
                    break;
                }
            }
        }

        Console.WriteLine($"Client: {request.Client}");

        HashSet<string> permissions = request.Writes.Select(x => x.Key).ToHashSet()
            .Union(request.Reads.AsEnumerable()).ToHashSet();
        Lease lease = BuildLease(permissions);
        do
        {
            Console.WriteLine("Checking for necessary leases");
            bool hasLeases = CheckForNecessaryLeases(request.Reads, request.Writes);
            if (!hasLeases)
            {
                Console.WriteLine("Leases not available");

                CheckIfRequestWasAlreadyMade(lease);

                RequestLease(lease);
                requestedLeases.Add(new Tuple<Lease, int>(lease, 1)); // adds the lease to the requested leases
                lock (lockObject) 
                {
                    Monitor.Wait(lockObject); // waits for some other thread wake it up when new leases are available
                }
            }
            else break;
        } while (!CheckForNecessaryLeases(request.Reads, request.Writes));
        
        Console.WriteLine("Leases available");
        
        HashSet<DadInt> result;
        lock (lockObject) 
        {
            result = DoTransaction(request.Client, request.Reads, request.Writes); // execute transaction
        }

        DecreaseLeaseRequestCount(lease); // decreases the number of requests for this lease

        ReleaseLeaseIfNecessary(lease);

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
