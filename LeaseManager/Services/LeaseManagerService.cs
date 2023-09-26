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
    const string clientScriptFilename = "../configuration_sample";
    int timeSlot;
    int nOfSlots;
    // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    public List<LeaseRequest> requests = new List<LeaseRequest>();
    public ProposedValueAndTimestamp proposedValueAndTimestamp;
    Boolean isLeader = false;
    string node;
    HashSet<string> nodes;
    //Monitor monitor; // monitor to be used by the threads that are waiting for the paxos instance to end

    public LeaseManagerService(string node, HashSet<string> nodes)
    {
        this.node = node;
        this.nodes = nodes;
        this.proposedValueAndTimestamp = proposedValueAndTimestamp;
    }
    void ProcessConfigurationFile()
    {
        IEnumerator<string> lines = File.ReadLines(clientScriptFilename).GetEnumerator();
        while (lines.MoveNext())
        {
            string line = lines.Current;
            if (!line.StartsWith('#')) // not a comment
            {
                string[] split = line.Split(' ');
                string firstToken = split[1];
                /*
                if (line.StartsWith('P'))
                {
                    string type = split[2];
                    if (type == "L")
                    {
                        string name = firstToken;
                        int href = int.Parse(split[3]);
                        // TODO: create a new lease manager
                    }
                }
                */
                if(line.StartsWith('D'))
                {
                    timeSlot = int.Parse(firstToken);
                }
                else if(line.StartsWith('S'))
                {
                    nOfSlots = int.Parse(firstToken);
                }
            }
        }
    }

    public override Task<RequestLeaseReply> RequestLease(RequestLeaseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        
        requests.Add(new LeaseRequest(request.TransactionManager, request.Permissions.ToHashSet()));

        Monitor.Wait(this); // waits for the paxos instance to end
        
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

    public void WakeSleepingThreads()
    {
        Console.WriteLine("Waking up sleeping threads");
        Monitor.PulseAll(this);
    }
}
