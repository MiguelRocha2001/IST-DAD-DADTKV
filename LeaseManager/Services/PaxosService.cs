namespace LeaseManager.Services;

using Grpc.Core;
using GrpcPaxos;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using GrpcDADTKV;
using System.Reflection;
using domain;
using AcceptedValue =  GrpcPaxos.PrepareReply.Types.AcceptedValue;
using System.Threading.Channels;

public class PaxosService : Paxos.PaxosBase
{
    LeaseManagerService leaseManagerService;
    const string clientScriptFilename = "../configuration_sample";
    int QUORUM_SIZE {get;} 
    int timeSlot;
    int nOfSlots;
    // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    int nodeId;
    GrpcChannel[] nodes;
    int currentEpoch = 1; // FIXME: use concurrency bullet proof
    int currentEpochId;
    int promisedEpochId;
    object lockAcceptedValue = new object();

    AcceptedValue? acceptedValue = null;
    
    public PaxosService(int nodeId, GrpcChannel[] nodes, LeaseManagerService leaseManagerService)
    {
        Console.WriteLine(nodeId);

        this.QUORUM_SIZE = nodes.Count() / 2 + 1;
        this.nodeId = nodeId;
        //this.nodes = nodes; TODO: Uncomment
        this.nodes = new GrpcChannel[]{
            GrpcChannel.ForAddress("http://localhost:6001"),
            GrpcChannel.ForAddress("http://localhost:6002"),
        };
        this.leaseManagerService = leaseManagerService;

        //ProcessConfigurationFile();
        //Console.WriteLine("Rquests: " + test.requests);
    }
    public void Init(){
        if (IsLeader())
        {
            Console.WriteLine("Aqui");
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



    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        Console.WriteLine("Prepare request received");
        if (promisedEpochId < request.Id)
        {
            promisedEpochId = request.Id; // overrides the last leader promised epoch id
            PrepareReply prepareReply = new PrepareReply{ Id = promisedEpochId};

            if (acceptedValue is not null)
                prepareReply.AcceptedValue = acceptedValue;     

            return Task.FromResult(prepareReply);
        }

        return Task.FromResult(new PrepareReply{
            Id = promisedEpochId
        }); 
    }

    /*public override Task<PromiseReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        /*
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
    }*/

    public override Task<Empty> Accept(AcceptRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Accept request received with {request.Value}");
        // if (request.Id < curre)
        /*
        int curReadTimestamp = leaseManagerService.proposedValueAndTimestamp.readTimestamp;
        //Console.WriteLine("curReadTimestamp: " + curReadTimestamp);
        //Console.WriteLine("request.WriteTimestamp: " + request.WriteTimestamp);

        AcceptReply acceptReply = new AcceptReply();
        if (curReadTimestamp < request.WriteTimestamp) // accept this value
        {
            // overrides the paxos chosen value with the one from the leader (inside the accept request)
            DefineNewValue(request.Leases.ToList(), request.WriteTimestamp, nodeId);
        
            // TODO: also send accepted reply to all learners
            
            // TODO: warn lease manager request is accepted
        }

        return Task.FromResult(new AcceptReply{});*/
        return Task.FromResult(new Empty());
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
        /*
        Console.WriteLine("Accepted request received");

        accepted++; // one more accept received
        if (accepted >= nodes.Count / 2) // majority
        {
            // end of paxos instance (Decide)
            
        }
        */
        return Task.FromResult(new AcceptedReply{});
    }

    private void  BroadcastPrepareRequest()
    {
        Console.WriteLine("BroadCast Start Prepare");
        var prepareRequest = new PrepareRequest
        {
            Epoch = currentEpoch,
            Id = currentEpochId,
        };
        //List<AsyncUnaryCall<PrepareReply>> replies = new(nodes.Count());

        int count = 1;        
        //AcceptedValue? acceptedValue = null;

        for (int id = 0; id < nodes.Count(); id++)
        {
            // TODO: verify if node is sus
            Console.WriteLine("Before continue");
            if (id == nodeId)
                continue;
            Console.WriteLine("After continue");
            Task.Run(async () => {

                Console.WriteLine("Broadcasting PrepareRequest to node " + id);
                var client = new Paxos.PaxosClient(nodes[id]);
                var reply = await client.PrepareAsync(prepareRequest);

                // Prepare Request not accepted
                if (reply.Id > currentEpochId)
                {
                    // TODO: Retry broadcast prepare
                    return;
                }

                if (count == QUORUM_SIZE) return; // no more promise msg processing required

                Interlocked.Increment(ref count);

                // Overrides the current value with the one from the acceptor
                if (reply.AcceptedValue is not null)
                {
                    lock (lockAcceptedValue)
                    {
                        if (acceptedValue is null)
                        {
                            acceptedValue = reply.AcceptedValue;
                        }
                        else if (acceptedValue.Id < reply.AcceptedValue.Id)
                        {
                            acceptedValue = reply.AcceptedValue;
                        }
                    }
                }

                if (count == QUORUM_SIZE)
                {
                    BroadcastAcceptRequest(acceptedValue);
                }
            });   

        }
    }

    /**
        Broadcasts the AcceptRequest to all nodes.
    */
    void BroadcastAcceptRequest(AcceptedValue? acceptedValue)
    {
        var leases = acceptedValue is null ? GenerateLeases() : acceptedValue.Value;

        AcceptRequest acceptRequest = new AcceptRequest{
            Id = currentEpochId,
            Value = leases
        };

        for (int id = 0; id < nodes.Count(); id++)
        {
            if (id == nodeId)
                continue;
        
            GrpcChannel channel = nodes[id];
            var client = new Paxos.PaxosClient(channel);
            Console.WriteLine("Broadcasting AcceptRequest");
            client.AcceptAsync(acceptRequest);
        }
    }
    private string GenerateLeases(){
        return $"Node: {nodeId}";
    }
    /**
        Builds a Grpc.Paxosleases from the current proposed value.
    */
    private List<GrpcPaxos.Lease> GenerateLeases2()
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
