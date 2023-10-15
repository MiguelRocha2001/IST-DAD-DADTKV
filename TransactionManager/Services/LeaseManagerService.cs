namespace TransactionManager.Services;

using Grpc.Core;
using GrpcLeaseService;

public class LeaseManagerService : LeaseService.LeaseServiceBase
{
    DadTkvService dadTkvService;

    public LeaseManagerService(DadTkvService dadTkvService)
    {
        this.dadTkvService = dadTkvService;
    }

    public override Task<Empty> SendLeasesOrder(LeasesResponse request, ServerCallContext context)
    {
        /*
        HashSet<Lease> ExtractTransactionManagerLeases()
        {
            HashSet<Lease> transactionManagerLeases = new HashSet<Lease>();
            foreach (Lease lease in request.Leases)
            {
                if (lease.TransactionManagerId == dadTkvService.nodeUrl)
                    transactionManagerLeases.Add(lease);
            }
            return transactionManagerLeases;
        }
        */

        /**
            Removes all leases that are conflicted with the new ones
        */
        void RemoveConflictedLeases(IEnumerable<Lease> leases)
        {
            foreach (Lease lease in leases)
            {
                if (lease.TransactionManagerId == dadTkvService.nodeUrl)
                    continue;
                else
                {
                    foreach (Lease localLease in dadTkvService.leases)
                    {
                        foreach (string requestId in localLease.RequestIds)
                        {
                            if (lease.RequestIds.Contains(requestId))
                            {
                                dadTkvService.leases.Remove(localLease);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Console.WriteLine("Received new assigment of leases");

        Console.WriteLine("LeasesOrder: " + request.LeaseOrder);

        // RemoveConflictedLeases(request.LeaseOrder);
        
        // HashSet<Lease> leases = ExtractTransactionManagerLeases();
        /*
        foreach (Lease lease in leases) // update leases
        {
            if (lease.TransactionManagerId == dadTkvService.nodeUrl)
                dadTkvService.leases.Add(lease);
        }

        dadTkvService.WakeUpWaitingTransactionRequests();
        */
        return Task.FromResult(new Empty());
    }

    public override Task<Empty> RequestLease(Lease request, ServerCallContext context)
    {
        throw new NotImplementedException();
    }
}
