namespace TransactionManager.Services;

using Grpc.Core;
using GrpcLeaseService;
using System.Collections;
using System.Collections.Concurrent;
using System.Security.AccessControl;

class ListLeaseComparator : EqualityComparer<List<Lease>>
{
    public override bool Equals(List<Lease>? l1, List<Lease>? l2) =>
        StructuralComparisons.StructuralEqualityComparer.Equals(l1?.ToArray(), l2?.ToArray());

    public override int GetHashCode(List<Lease> l) =>
        StructuralComparisons.StructuralEqualityComparer.GetHashCode(l.ToArray());
}

public class LeaseManagerService : LeaseService.LeaseServiceBase
{
    DadTkvService dadTkvService;
    private int quorumSize;
    ConcurrentDictionary<List<Lease>, int> acceptedValues = new(new ListLeaseComparator());
    List<Lease> leases = new();

    public LeaseManagerService(DadTkvService dadTkvService, int quorumSize)
    {
        this.dadTkvService = dadTkvService;
        this.quorumSize = quorumSize;
    }

    public override Task<Empty> SendLeases(LeasesResponse request, ServerCallContext context)
    {
        Console.WriteLine("Received new assigment of leases");
        Console.WriteLine("Leases: " + request.Leases);

        int count = acceptedValues.AddOrUpdate(request.Leases.ToList(), 1, (k, v) => v + 1);
        if (count == quorumSize)
        {
            Console.WriteLine("Quorum reached");
            leases = request.Leases.ToList();
            UpdateLeases();
            dadTkvService.WakeUpWaitingTransactionRequests(); // wakes waiting transactions
        }

        return Task.FromResult(new Empty());
    }

    public void LeaseReleased(Lease releasedLease)
    {
        leases.Remove(releasedLease);
        UpdateLeases();
    }

    public void UpdateLeases()
    {
        bool ConflictsWith(Lease l1, Lease l2) =>
            l1.RequestIds.Intersect(l2.RequestIds).Any();

        void ComputeLeases(List<Lease> selfLeaseRequests)
        {
            foreach (Lease lease in selfLeaseRequests)
            {
                foreach (Lease lease1 in leases)
                {
                    // some other TM has a lease that conflicts with this one, with greater priority
                    if (!lease.Equals(lease1) && ConflictsWith(lease, lease1))
                    {
                        break;
                    }

                    // we have priority
                    if (lease.Equals(lease1))
                    {
                        bool releaseThisLeaseAfterTransaction = false;

                        // check if there other conflicting leases after this one
                        if (leases.Where(l => !l.Equals(lease)).Any(l => ConflictsWith(lease, l)))
                        {
                            releaseThisLeaseAfterTransaction = true;
                        }

                        dadTkvService.leases.Add(new Tuple<bool, Lease>(releaseThisLeaseAfterTransaction, lease));
                        dadTkvService.WakeUpWaitingTransactionRequests(); // wakes waiting transactions
                        break;
                    }
                }
            }
        }



        Console.WriteLine("Updating leases");

        foreach (Lease lease in leases)
        {
            // if lease is not ours and we have one with the same permissions, remove it
            if (lease.TransactionManagerId != dadTkvService.nodeId.ToString())
            {
                if (dadTkvService.leases.Where(l => ConflictsWith(l.Item2, lease)).Any())
                {
                    dadTkvService.leases.Remove(dadTkvService.leases.Where(l => ConflictsWith(l.Item2, lease)).First()); // removes lease
                    Console.WriteLine("Lease with permissions " + lease.RequestIds + " removed");
                }
            }

            // select leases that belong to this node
            List<Lease> selfLeaseRequests = leases.Where(l => l.TransactionManagerId == dadTkvService.nodeId.ToString()).ToList();
            ComputeLeases(selfLeaseRequests);
        }
        dadTkvService.WakeUpWaitingTransactionRequests(); // wakes waiting transactions
    }
}
