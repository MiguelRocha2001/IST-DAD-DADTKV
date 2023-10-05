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
    private List<LeaseRequest> requests;
    AcceptedValue? acceptedValue;
    
    public LeaseManagerService(List<LeaseRequest> requests, AcceptedValue? acceptedValue)
    {
        this.requests = requests;
        this.acceptedValue = acceptedValue;
    }
    
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
            
            foreach (var leaseAux in acceptedValue.Leases)
            {
                GrpcDADTKVLease.Lease lease = new GrpcDADTKVLease.Lease();
                lease.TransactionManager = leaseAux.TransactionManager;
                lease.Permissions.Add(leaseAux.Permissions.ToArray());
                leases.Add(lease);
            }
            
            return Task.FromResult(reply);   
        }
    }
}
