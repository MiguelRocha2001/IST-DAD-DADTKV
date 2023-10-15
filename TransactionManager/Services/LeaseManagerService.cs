namespace TransactionManager.Services;

using Grpc.Core;
using GrpcLeaseService;
using System.Collections;
using System.Collections.Concurrent;
using System.Security.AccessControl;

class ListLeaseOrderComparator : EqualityComparer<List<LeaseOrder>>
{
    public override bool Equals(List<LeaseOrder>? l1, List<LeaseOrder>? l2) =>
        StructuralComparisons.StructuralEqualityComparer.Equals(l1?.ToArray(), l2?.ToArray());

    public override int GetHashCode(List<LeaseOrder> l) =>
        StructuralComparisons.StructuralEqualityComparer.GetHashCode(l.ToArray());
}

public class LeaseManagerService : LeaseService.LeaseServiceBase
{
    DadTkvService dadTkvService;
    private int quorumSize;
    ConcurrentDictionary<List<LeaseOrder>, int> acceptedValues = new(new ListLeaseOrderComparator());
    List<LeaseOrder> requestedLeasesOrder; // holds the requested leases and the respective order

    public LeaseManagerService(DadTkvService dadTkvService, int quorumSize)
    {
        this.dadTkvService = dadTkvService;
        this.quorumSize = quorumSize;
    }

    public override Task<Empty> SendLeasesOrder(LeasesResponse request, ServerCallContext context)
    {
        Console.WriteLine("Received new assigment of leases");
        Console.WriteLine("LeasesOrder: " + request.LeaseOrder);

        int count = acceptedValues.AddOrUpdate(request.LeaseOrder.ToList(), 1, (k, v) => v + 1);
        if (count == quorumSize)
        {
            Console.WriteLine("Quorum reached");
            requestedLeasesOrder = request.LeaseOrder.ToList();
            UpdateLeases();
            dadTkvService.WakeUpWaitingTransactionRequests(); // wakes waiting transactions
        }

        return Task.FromResult(new Empty());
    }

    public void LeaseReleased(Lease releasedLease)
    {
        bool ConflictsWith(Lease l1, Lease l2) =>
            l1.RequestIds.Intersect(l2.RequestIds).Any();

        void DecreaseOrder(LeaseOrder leaseOrder)
        {
            foreach (LeaseOrder l in requestedLeasesOrder)
            {
                if (l.Lease.Equals(leaseOrder.Lease))
                {
                    l.Order--;
                    break;
                }   
            }
            leaseOrder.Order--;
        }

        Console.WriteLine("LeaseReleased: " + releasedLease);
        foreach (LeaseOrder leaseOrder in requestedLeasesOrder)
        {
            if (ConflictsWith(releasedLease, leaseOrder.Lease))
                DecreaseOrder(leaseOrder);  
        }
        UpdateLeases();
    }

    public void UpdateLeases()
    {
        Console.WriteLine("Updating leases");
        foreach (LeaseOrder leaseOrder in requestedLeasesOrder)
        {
            if (leaseOrder.Order == 0)
                dadTkvService.leases.Add(leaseOrder.Lease);
        }
        dadTkvService.WakeUpWaitingTransactionRequests(); // wakes waiting transactions
    }
}
