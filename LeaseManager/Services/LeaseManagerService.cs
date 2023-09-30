namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKVLease;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using GrpcPaxos;
using domain;

public class LeaseManagerService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    public List<LeaseRequest> requests = new List<LeaseRequest>();

    public override Task<RequestLeaseReply> RequestLease(RequestLeaseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        lock (this)
        {
            requests.Add(new LeaseRequest(request.TransactionManager, request.Permissions.ToHashSet()));

            Console.WriteLine($"Going to wait for a decision");
            Monitor.Wait(this);
            Console.WriteLine($"Decision received");
            
            // decided value is now available
            RequestLeaseReply reply = new RequestLeaseReply();
            
            // builds the reply
            List<GrpcDADTKVLease.Lease> leases = new List<GrpcDADTKVLease.Lease>();
            //LeaseAtributionOrder decidedValue = proposedValueAndTimestamp.value;

            reply.Value = "JUST_A_TEST";
            
            /*
            foreach (Tuple<int, LeaseRequest> tuple in decidedValue.leases)
            {
                GrpcDADTKV.Lease lease = new GrpcDADTKV.Lease();
                lease.Permissions.Add(tuple.Item2.permissions);
                lease.Epoch = tuple.Item1;
                lease.TransactionManager = tuple.Item2.transactionManager;
                leases.Add(lease);
            }
            reply.Leases.Add(leases);
            */

            return Task.FromResult(reply);   
        }
    }
}
