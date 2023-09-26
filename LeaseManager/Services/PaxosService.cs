namespace LeaseManager.Services;

using Grpc.Core;
using GrpcPaxos;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using GrpcDADTKV;
using System.Reflection;
using domain;

public class PaxosService : Paxos.PaxosBase
{
    LeaseManagerService leaseManagerService;
    const string clientScriptFilename = "../configuration_sample";
    int timeSlot;
    int nOfSlots;
    // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    bool isLeader = false;
    string previousLeader = "";
    string node;
    HashSet<string> nodes;
    int promised = 0; // FIXME: use concurrency bullet proof
    int accepted = 0; // FIXME: use concurrency bullet proof
    int currentEpoch = 1; // FIXME: use concurrency bullet proof

    List<string> liders = new List<string>();

    
    public PaxosService(string node, HashSet<string> nodes, LeaseManagerService leaseManagerService)
    {
        Console.WriteLine("PaxosService constructor");
        this.node = node;
        this.nodes = nodes;
        this.leaseManagerService = leaseManagerService;

        //ProcessConfigurationFile();
        //Console.WriteLine("Rquests: " + test.requests);
    
        void BroadcastPrepareRequest()
        {
            foreach (string node in nodes)
            {
                if (!node.Equals(this.node))
                {
                    Console.WriteLine("Broadcasting PrepareRequest to node " + node);
                    using var channel = GrpcChannel.ForAddress(node);
                    var client = new Paxos.PaxosClient(channel);
                    client.Prepare(new PrepareRequest());
                }
            }
        }

        /**
            Orders leaders by port number and defines the leader.
        */
        void OrderLeaders()
        {
            List<int> ports = new List<int>();
            foreach (string node in nodes)
            {
                string[] split = node.Split(':');
                string port = split[2];
                ports.Add(int.Parse(port));
            }
            ports.Sort();
            foreach (int port in ports)
            {
                foreach (string node in nodes)
                {
                    if (node.EndsWith(port.ToString()))
                    {
                        liders.Add(node);
                    }
                }
            }
            isLeader = liders[0].Equals(node);
            if (!isLeader)
            {
                previousLeader = liders[0];
            }
        }

        DefineNewValue();
        OrderLeaders();

        Thread.Sleep(2000); // wall time (make it configurable in the future)
        if (isLeader && previousLeader != "")
        {
            BroadcastPrepareRequest();
        }
        else if (isLeader)
        {
            BroadcastAcceptRequest();
        }
    }
    
    private void ProcessConfigurationFile()
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

    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        Console.WriteLine("Prepare request received");

        promised++; // one more promise accepted

        if (promised >= nodes.Count / 2) // majority
        {
            BroadcastAcceptRequest();
        }
        return Task.FromResult(new PrepareReply{});
    }

    public override Task<PromiseReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        Console.WriteLine("Promise request received");
        int requestTimestamp = request.WriteTimestamp;
        int valueToBeProposed = leaseManagerService.proposedValueAndTimestamp.writeTimestamp;
        if (requestTimestamp > valueToBeProposed)
        {
            DefineNewValue(request.Value.ToList(), requestTimestamp);
        }
        if (promised > nodes.Count / 2) // majority
        {
            BroadcastAcceptRequest();
        }
        
        return Task.FromResult(new PromiseReply{});
    }

    public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    {
        Console.WriteLine("Accept request received");

        // TODO: implement this

        return Task.FromResult(new AcceptReply{});
    }

    /**
        Overrides the paxos chosen value and sets read and write timestamps to 1.
    */
    private void DefineNewValue()
    {
        List<LeaseRequest> leaseRequests = leaseManagerService.requests;
        LeaseAtributionOrder chosenOrder = new LeaseAtributionOrder();

        foreach (LeaseRequest leaseRequest in leaseRequests)
        {        
            if (chosenOrder.Contains(leaseRequest))
            {
                int maxEpoch = chosenOrder.GetGreatestAssignedLeaseEpoch(leaseRequest); // should be =/= -1
                chosenOrder.AddLease(maxEpoch, leaseRequest);
            }
            else
            {
                chosenOrder.AddLease(currentEpoch, leaseRequest);
            }
        }
        leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, 1, 1);
    }

    /**
        Overrides the paxos chosen value
    */
    private void DefineNewValue(List<GrpcPaxos.Lease> leases, int writeTimestamp)
    {
        List<LeaseRequest> leaseRequests = new List<LeaseRequest>();
        foreach (GrpcPaxos.Lease lease in leases)
        {
            leaseRequests.Add(new LeaseRequest(lease.TransactionManager, lease.Permissions.ToHashSet()));
        }
        LeaseAtributionOrder chosenOrder = new LeaseAtributionOrder();

        foreach (LeaseRequest leaseRequest in leaseRequests)
        {        
            if (chosenOrder.Contains(leaseRequest))
            {
                int maxEpoch = chosenOrder.GetGreatestAssignedLeaseEpoch(leaseRequest); // should be =/= -1
                chosenOrder.AddLease(maxEpoch, leaseRequest);
            }
            else
            {
                chosenOrder.AddLease(currentEpoch, leaseRequest);
            }
        }
        leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, writeTimestamp, 1);
    }
    

    public override Task<AcceptedReply> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        accepted++; // one more accept received
        if (accepted >= nodes.Count / 2) // majority
        {
            // end of paxos instance (Decide)
            
            leaseManagerService.WakeSleepingThreads(); // wakes threads that are waiting for the paxos instance to end
        }
        return Task.FromResult(new AcceptedReply{});
    }

    /**
        Broadcasts the AcceptRequest to all nodes.
    */
    void BroadcastAcceptRequest()
    {
        List<GrpcPaxos.Lease> BuildLeases()
        {
            List<GrpcPaxos.Lease> leases = new List<GrpcPaxos.Lease>();
            LeaseAtributionOrder value = leaseManagerService.proposedValueAndTimestamp.value;

            foreach (Tuple<int, LeaseRequest> assigment in value.leases)
            {
                GrpcPaxos.Lease grpcLease = new GrpcPaxos.Lease();
                grpcLease.TransactionManager = assigment.Item2.transactionManager;
                grpcLease.Permissions.AddRange(assigment.Item2.permissions);
                leases.Add(grpcLease);
            }
            
            return leases;
        }

        Console.WriteLine("Broadcasting AcceptRequest");
        foreach (string node in nodes)
        {
            if (!node.Equals(this.node))
            {
                using var channel = GrpcChannel.ForAddress(node);
                var client = new Paxos.PaxosClient(channel);

                AcceptRequest request = new AcceptRequest();
                request.WriteTimestamp = leaseManagerService.proposedValueAndTimestamp.writeTimestamp;
                request.Leases.AddRange(BuildLeases());
                client.Accept(request);
            }
        }
    }
}
