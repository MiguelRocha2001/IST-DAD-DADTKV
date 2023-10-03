namespace LeaseManager.Services;

using Grpc.Core;
using GrpcPaxos;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using domain;
using AcceptedValue = GrpcPaxos.AcceptedValue;

/**
    Notes:
        - When the proposer broadcasts accept request, it increments the accepted value because the proposer accepts its own value.
        - When a acceptor receives an accept request, it increments the accepted value, checks if the quorum was reached and broadcasts the accepted message.
        - Node only generates value when needed (if value is not yet generated and node needs to broadcast accept request);
    TODO:
        - A Proposer should not initiate Paxos if it cannot communicate with at least a Quorum of Acceptors.
        - Take precausion against cuncurrency
*/
public class PaxosService : Paxos.PaxosBase
{
    LeaseManagerService leaseManagerService;
    const string clientScriptFilename = "../configuration_sample";
    readonly int QUORUM_SIZE;
    int timeSlot;
    int nOfSlots;
    // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    int nodeId;
    GrpcChannel[] nodes;
    bool finishedPaxosEpoch = false;
    int currentEpoch = 1;
    int currentEpochId;
    int promisedEpochId;
    object lockAcceptedValue = new object();
    AcceptedValue? acceptedValue = null;
    int accepted = 0;

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

    public async void Init()
    {
        CancellationTokenSource tokenSource = new();
        CancellationToken token = new();

        while (true)
        {
            Task task = Task.Run(() =>
            {
                if (IsLeader())
                    BroadcastPrepareRequest(tokenSource, token);
            }, token);
            await Task.Delay(5000);
            Console.WriteLine("\n\n starting new paxos instance \n\n");
        }
    }

    private bool IsLeader()
    {
        for (int id = 0; id < nodes.Count(); id++)
        {
            if (!IsSuspect(id))
                return id == nodeId;
        }
        return false;
    }

    private bool IsSuspect(int id)
    {
        return id != 0;
    }

    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        Console.WriteLine("Prepare request received");

        if (promisedEpochId < request.Id)
        {
            promisedEpochId = request.Id; // overrides the last leader promised epoch id
            accepted = 0; // resets the accepted count
            PrepareReply prepareReply = new PrepareReply { Id = promisedEpochId };

            if (acceptedValue is not null)
                prepareReply.AcceptedValue = acceptedValue;

            return Task.FromResult(prepareReply);
        }

        return Task.FromResult(new PrepareReply
        {
            Id = promisedEpochId
        });
    }

    public override Task<Empty> Accept(AcceptRequest request, ServerCallContext context)
    {
        /**
            Broadcasts the Accepted message to all nodes, except self.
        */
        void BroadcastAcceptedMsg()
        {
            for (int id = 0; id < nodes.Count(); id++)
            {
                if (id == nodeId)
                {
                    Console.WriteLine("Skipping node " + id + " (self)");
                    continue;
                }
                GrpcChannel channel = nodes[id];
                AcceptedRequest acceptedRequest = new AcceptedRequest
                {
                    AcceptedValue = acceptedValue,
                };
                var client = new Paxos.PaxosClient(channel);
                client.AcceptedAsync(acceptedRequest);
                Console.WriteLine("Accepted message sent to node " + id);
            }
        }

        Console.WriteLine($"Accept request received from {request.AcceptedValue.Id}");

        int requestId = request.AcceptedValue.Id;

        // Already promised to another younger leader
        if (requestId < promisedEpochId)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Already promised to another younger leader"));
        }
        else
        {
            // Overrides the current local value with the one from the proposer
            acceptedValue = request.AcceptedValue;

            Console.WriteLine("Accepted count: " + accepted);
            Console.WriteLine("Quorum size: " + QUORUM_SIZE);

            // increments the accepted value and checks if the quorum was reached
            if (++accepted == QUORUM_SIZE - 1) // minus one because the proposer already accepted its own value
            {
                Console.WriteLine("Quorum reached");
                InformLeaseManagerOnPaxosEnd();
                finishedPaxosEpoch = true;
            }

            BroadcastAcceptedMsg();
        }
        return Task.FromResult(new Empty());
    }


    public override Task<Empty> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        Console.WriteLine("Accepted request received");

        /*
        if (IsLeader())
            Console.WriteLine("Accepted request received from acceptor");
        else // Learner
            Console.WriteLine("Accepted request received from proposer");
        */

        // end of paxos instance (Decide)
        if (++accepted == QUORUM_SIZE)
        {
            Console.WriteLine("Quorum reached");
            InformLeaseManagerOnPaxosEnd();
            finishedPaxosEpoch = true;
        }

        return Task.FromResult(new Empty { });
    }

    private void InformLeaseManagerOnPaxosEnd()
    {
        // Decide
        lock (leaseManagerService) // aquires lock on the lease manager service, so its possible to wake pending threads
        {
            Monitor.PulseAll(leaseManagerService); // wakes pending threads
            Console.WriteLine("Lease Manager notified on end of paxos instance");
        }
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
        //leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, 1, 0);
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
        //leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, writeTimestamp, readTimestamp);
    }

    private void BroadcastPrepareRequest(CancellationTokenSource tokenSource, CancellationToken token)
    {
        Console.WriteLine("Prepare BroadCast Start");

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
            if (id == nodeId)
            {
                //Console.WriteLine($"Skipping node {id}");
                continue;
            }

            int idAux = id; // this is because the [id] variable is not captured by the lambda expression
            Task.Run(async () =>
            {
                Console.WriteLine("Broadcasting PrepareRequest to node " + idAux);

                try
                {
                    var client = new Paxos.PaxosClient(nodes[idAux]);
                    var reply = await client.PrepareAsync(prepareRequest);
                    Console.WriteLine("PrepareReply received from node " + idAux);

                    if (reply.Id > currentEpochId)
                    {
                        // Restart BroadcastPrepareRequest
                        tokenSource.Cancel();
                        return;
                    }

                    if (count >= QUORUM_SIZE) return; // no more promise msg processing required

                    if (reply.AcceptedValue is not null)
                    {
                        lock (lockAcceptedValue)
                        {
                            // Overrides the current value with the one from the acceptor
                            if (acceptedValue is null || acceptedValue.Id < reply.AcceptedValue.Id)
                            {
                                acceptedValue = reply.AcceptedValue;
                                Console.WriteLine($"Local value updated to {acceptedValue}");
                            }
                        }
                    }

                    // Double lock mechanism to prevent multiple BroadCastAcceptRequests
                    if (count < QUORUM_SIZE)
                    {
                        lock (lockAcceptedValue)
                        {
                            count++;
                            if (count == QUORUM_SIZE)
                                BroadcastAcceptRequest();
                        }
                    }
                }
                catch (System.Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw e;
                }
            }, token);
        }
    }

    /**
        Broadcasts the AcceptRequest to all nodes.
        Increments the accepted value because the proposer accepts its own value.
    */
    void BroadcastAcceptRequest()
    {
        if (acceptedValue is null) // generate local value if necessary
            GenerateNewValue();

        AcceptRequest acceptRequest = new AcceptRequest
        {
            AcceptedValue = acceptedValue
        };

        Console.WriteLine("Broadcasting AcceptRequest");
        for (int id = 0; id < nodes.Count(); id++)
        {
            if (id == nodeId)
            {
                Console.WriteLine("Skipping node " + id + " (self)");
                continue;
            }

            GrpcChannel channel = nodes[id];
            var client = new Paxos.PaxosClient(channel);
            client.AcceptAsync(acceptRequest);
            Console.WriteLine("AcceptRequest sent to node " + id);
        }

        accepted++; // the proposer accepts its own value
    }

    // FIXME: adapt to new lease manager
    private string GenerateLeases2()
    {
        return $"Node: {nodeId}";
    }
    /**
        Builds a Grpc.Paxosleases from the current proposed value.
    */
    private void GenerateNewValue()
    {
        List<GrpcPaxos.Lease> leases = new List<GrpcPaxos.Lease>();
        acceptedValue = new AcceptedValue
        {
            Id = currentEpochId,
            Leases = { leases }
        };
        /*
        LeaseAtributionOrder value = leaseManagerService.proposedValueAndTimestamp.value;

        foreach (Tuple<int, LeaseRequest> assigment in value.leases)
        {
            GrpcPaxos.Lease grpcLease = new GrpcPaxos.Lease();
            grpcLease.TransactionManager = assigment.Item2.transactionManager;
            grpcLease.Permissions.AddRange(assigment.Item2.permissions);
            leases.Add(grpcLease);
        }
        */
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
                if (line.StartsWith('D'))
                {
                    timeSlot = int.Parse(firstToken);
                }
                else if (line.StartsWith('S'))
                {
                    nOfSlots = int.Parse(firstToken);
                }
            }
        }
    }

    private GrpcChannel GetNodeChannel(int id)
    {
        return nodes[id % nodes.Count()];
    }
}
