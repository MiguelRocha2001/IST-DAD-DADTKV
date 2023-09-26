namespace domain;

public class LeaseAtributionOrder
{
    HashSet<Tuple<string, Lease> leases;

    public LeaseAtributionOrder(HashSet<Tuple<string, Lease> leases)
    {
        this.leases = leases;   
    }
}

class Lease
{
    public HashSet<string> permissions;

    public Lease(HashSet<string> permissions)
    {
        this.permissions = permissions;
    }
}

interface IPaxosStep {}
class Step1 : IPaxosStep {
    int epoch;
    string value;

    public Step1(int epoch, string value)
    {
        this.epoch = epoch;
        this.value = value;
    }
}
class Step2 : IPaxosStep {}