namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using GrpcPaxos;
using domain;

public class LeaseManagerService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    public List<LeaseRequest> requests = new List<LeaseRequest>();
    public ProposedValueAndTimestamp proposedValueAndTimestamp;
    //Monitor monitor; // monitor to be used by the threads that are waiting for the paxos instance to end

    public override Task<RequestLeaseReply> RequestLease(RequestLeaseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        
        requests.Add(new LeaseRequest(request.TransactionManager, request.Permissions.ToHashSet()));

        int writeTimestamp = proposedValueAndTimestamp.writeTimestamp;

        do
        {
            Monitor.Wait(this);
        } while (proposedValueAndTimestamp.writeTimestamp == writeTimestamp);
        
        // decided value is now available
        RequestLeaseReply reply = new RequestLeaseReply();
        
        // builds the reply
        List<GrpcDADTKV.Lease> leases = new List<GrpcDADTKV.Lease>();
        LeaseAtributionOrder decidedValue = proposedValueAndTimestamp.value;
        
        foreach (Tuple<int, LeaseRequest> tuple in decidedValue.leases)
        {
            GrpcDADTKV.Lease lease = new GrpcDADTKV.Lease();
            lease.Permissions.Add(tuple.Item2.permissions);
            lease.Epoch = tuple.Item1;
            lease.TransactionManager = tuple.Item2.transactionManager;
            leases.Add(lease);
        }
        
        reply.Leases.Add(leases);
        return Task.FromResult(reply);
    }
}
