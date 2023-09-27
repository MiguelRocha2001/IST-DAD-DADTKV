namespace domain;

public class LeaseAtributionOrder
{
    public HashSet<Tuple<int, LeaseRequest>> leases = new(); // epoch, leaseRequest
    
    public void AddLease(int epoch, LeaseRequest leaseRequest)
    {
        leases.Add(new Tuple<int, LeaseRequest>(epoch, leaseRequest));
    }
    
    public bool Contains(LeaseRequest leaseRequest)
    {
        foreach (Tuple<int, LeaseRequest> tuple in leases)
        {
            if (tuple.Item2 == leaseRequest)
            {
                return true;
            }
        }
        return false;
    }

    public int GetGreatestAssignedLeaseEpoch(LeaseRequest leaseRequest)
    {
        List<int> epochs = new List<int>();
        foreach (Tuple<int, LeaseRequest> tuple in leases)
        {
            if (tuple.Item2 == leaseRequest)
            {
                epochs.Add(tuple.Item1);
            }
        }
        return epochs.Max();
    }
}

/*
public class Lease
{
    public HashSet<string> permissions;

    public Lease(HashSet<string> permissions)
    {
        this.permissions = permissions;
    }
}
*/

public class LeaseRequest
{
    public string transactionManager;
    public HashSet<string> permissions;

    public LeaseRequest(string transactionManager, HashSet<string> permissions)
    {
        this.transactionManager = transactionManager;
        this.permissions = permissions;
    }
}

public class ProposedValueAndTimestamp
{
    public LeaseAtributionOrder value;
    public int writeTimestamp;
    public int readTimestamp;

    public ProposedValueAndTimestamp(LeaseAtributionOrder value, int writeTimestamp, int readTimestamp)
    {
        this.value = value;
        this.writeTimestamp = writeTimestamp;
        this.readTimestamp = readTimestamp;
    }
}

public class Liders
{
    Grpc.Net.Client.GrpcChannel[] liders
    List<string> liders = new List<string>();
    public string currentLider;
    
    public Liders(List<string> liders)
    {
        this.liders = liders;
        currentLider = liders[0];
    }

    public void AdvanceLider()
    {
        int index = liders.IndexOf(currentLider);
        if (index == liders.Count - 1)
        {
            currentLider = liders[0];
        }
        else
        {
            currentLider = liders[index + 1];
        }
    }

    public string GetLeader(int round)
    {
        return liders[(round % liders.Count) - 1];
    }
}