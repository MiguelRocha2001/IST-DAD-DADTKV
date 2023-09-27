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
    int nodeId;
    GrpcChannel[] nodes;
    int promised = 0; // FIXME: use concurrency bullet proof
    int accepted = 0; // FIXME: use concurrency bullet proof
    int currentEpoch = 1; // FIXME: use concurrency bullet proof
    int currentEpochId;
    int myId;
    //Liders liders;

    
    public PaxosService(int nodeId, GrpcChannel[] nodes, LeaseManagerService leaseManagerService)
    {
        Console.WriteLine("PaxosService constructor");

        this.nodeId = nodeId;
        this.nodes = nodes;
        this.leaseManagerService = leaseManagerService;

        //ProcessConfigurationFile();
        //Console.WriteLine("Rquests: " + test.requests);
    

        //DefineNewValue();
    
        string value = $"node{nodeId}";
        Thread.Sleep(2000); // wall time (make it configurable in the future)

        if (IsLeader())
        {
            BroadcastPrepareRequest();
        }
    }

    private bool IsLeader()
    {
        for( int id = 0; id < nodes.Count(); id++)
        {
           if(!IsSuspect(id))
               return id == this.nodeId ? true : false;
        }
        return false;
    }
    private bool IsSuspect(int id){
        return id != 0;
    }


    private async void BroadcastPrepareRequest()
    {
        var prepareRequest = new PrepareRequest
        {
            Epoch = currentEpoch,
            Id = currentEpochId,
        };
        List<AsyncUnaryCall<PrepareReply>> replies = new();

        for (int id = 0; id < nodes.Count(); id++)
        {
            if (id == nodeId)
                continue;
            Console.WriteLine("Broadcasting PrepareRequest to node " + id);

            
            var client = new Paxos.PaxosClient(nodes[id]);
            //var response = client.PrepareAsync(prepareRequest);
            var response = client.PrepareAsync(prepareRequest);
        }
    }

    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        Console.WriteLine("Prepare request received");

        // check leader number
        // if leader number is greater than last leader number, send promise reply
        int curReadTimestamp = leaseManagerService.proposedValueAndTimestamp.readTimestamp;
        if (curReadTimestamp < request.Round)
        {
            // send promise reply
            string lider = liders.GetLeader(request.Round);
            using var channel = GrpcChannel.ForAddress(lider);
            var client = new Paxos.PaxosClient(channel);
            PromiseRequest promiseRequest = new PromiseRequest();
            promiseRequest.WriteTimestamp = leaseManagerService.proposedValueAndTimestamp.writeTimestamp; // adds the current write timestamp
            promiseRequest.Value.AddRange(BuildLeases()); // adds the current value
            client.Promise(promiseRequest);
        }
        return Task.FromResult(new PrepareReply{});
    }

    public override Task<PromiseReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        Console.WriteLine("Promise request received");
        promised++; // one more promise received

        int requestWriteTimestamp = request.WriteTimestamp; // this will be the new write timestamp (if value is used)
        
        int valueToBeProposed = leaseManagerService.proposedValueAndTimestamp.writeTimestamp; // this will be the new value (if value is used)
        if (requestWriteTimestamp > valueToBeProposed) 
        {
            DefineNewValue(request.Value.ToList(), requestWriteTimestamp, myId);
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

        int curReadTimestamp = leaseManagerService.proposedValueAndTimestamp.readTimestamp;
        //Console.WriteLine("curReadTimestamp: " + curReadTimestamp);
        Console.WriteLine("request.WriteTimestamp: " + request.WriteTimestamp);

        if (curReadTimestamp < request.WriteTimestamp) // accept this value
        {
            DefineNewValue(request.Leases.ToList(), request.WriteTimestamp, myId);
            // send accepted reply
            string leader = liders.GetLeader(request.WriteTimestamp);
            using var channel = GrpcChannel.ForAddress(leader);
            var client = new Paxos.PaxosClient(channel);
            AcceptedRequest acceptedRequest = new AcceptedRequest();
            acceptedRequest.WriteTimestamp = leaseManagerService.proposedValueAndTimestamp.writeTimestamp; // adds the current write timestamp
            acceptedRequest.Leases.AddRange(BuildLeases()); // adds the current value
            Console.WriteLine("Sending accepted request to " + leader);
            client.Accepted(acceptedRequest);
            // TODO: warn lease manager request is accepted
        }

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
        leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, 1, 0);
    }

    /**
        Overrides the paxos chosen value 
    */
    private void DefineNewValue(List<GrpcPaxos.Lease> leases, int writeTimestamp, int readTimestamp)
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
        leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, writeTimestamp, readTimestamp);
    }
    

    public override Task<AcceptedReply> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        Console.WriteLine("Accepted request received");

        accepted++; // one more accept received
        if (accepted >= nodes.Count / 2) // majority
        {
            // end of paxos instance (Decide)
            
        }
        return Task.FromResult(new AcceptedReply{});
    }

    /**
        Broadcasts the AcceptRequest to all nodes.
    */
    void BroadcastAcceptRequest()
    {
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

    /**
        Builds a Grpc.Paxosleases from the current proposed value.
    */
    private List<GrpcPaxos.Lease> BuildLeases()
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
}
