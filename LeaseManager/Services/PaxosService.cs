namespace LeaseManager.Services;

using Grpc.Core;
using GrpcPaxos;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using AcceptedValue = GrpcPaxos.AcceptedValue;
using GrpcLeaseService;

/**
    Notes:
        - When the proposer broadcasts accept request, it increments the accepted value because the proposer accepts its own value.
        - When a acceptor receives an accept request, it increments the accepted value, checks if the quorum was reached and broadcasts the accepted message.
        - Node only generates value when needed (if value is not yet generated and node needs to broadcast accept request);
    TODO:
        - A Proposer should not initiate Paxos if it cannot communicate with at least a Quorum of Acceptors.
        - Take precausion against cuncurrency
        - Increase currentEpochId when necessary
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
    int currentEpoch = 1;
    int currentEpochId;
    int promisedEpochId = -1;
    object lockAcceptedValue = new object();
    object lockCurrentEpochId = new object();
    object lockPrepareMethod = new object();
    AcceptedValue? acceptedValue;
    int accepted = 0;
    CancellationTokenSource tokenSource;
    CancellationToken ct;
    CancellationTokenSource acceptedTokenSource = new();
    CancellationToken acceptedCt;

    public PaxosService(int nodeId, GrpcChannel[] nodes, LeaseManagerService leaseManagerService)
    {
        Console.WriteLine(nodeId);

        this.QUORUM_SIZE = nodes.Count() / 2 + 1;
        Console.WriteLine(QUORUM_SIZE);
        this.nodeId = nodeId;
        this.nodes = nodes;
        // this.nodes = new GrpcChannel[]{
        //     GrpcChannel.ForAddress("http://localhost:6001"),
        //     GrpcChannel.ForAddress("http://localhost:6002"),
        // };
        this.leaseManagerService = leaseManagerService;
        this.currentEpochId = nodeId;
        this.tokenSource = new();
        this.ct = tokenSource.Token;
        this.acceptedTokenSource = new();
        this.acceptedCt = acceptedTokenSource.Token;
        //ProcessConfigurationFile();
        //Console.WriteLine("Rquests: " + test.requests);
    }

    public async void Init()
    {
        void ResetProperties()
        {
            accepted = 0;
            currentEpochId = nodeId;
            promisedEpochId = -1;
            currentEpoch++;
            acceptedValue = null;
            tokenSource.Cancel();
        }

        var epochTimeInterval = TimeSpan.FromSeconds(10);
        // TODO: calculate state
        bool retry = true;
        while (true)
        {
            retry = true;
            await Task.Run(async () =>
            {
                var startTime = System.DateTime.Now;
                Console.WriteLine($"[{nodeId}] Starting new paxos instance.");
                if (IsLeader())
                {
                    Console.WriteLine($"[{nodeId}] I am the leader.");
                    BroadcastPrepareRequest(tokenSource, ct);
                }

                // No need to cancel because it is assumed that the state only changes with
                // each epoch.
                var delay = epochTimeInterval - (DateTime.Now - startTime);

                Console.WriteLine($"[{nodeId}] Waiting {delay} till next epoch.");
                await Task.Delay(delay);
                retry = false;
                ResetProperties();
            }, ct);

            if (retry)
            {
                // Give time for other lease Managers to achieve progress
                await Task.Delay(2000);
            }
            tokenSource = new();
            ct = tokenSource.Token;
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
        Console.WriteLine($"[{nodeId}] PrepareRequest received from {request.Id % nodes.Count()}");

        lock (lockPrepareMethod)
        {

            if (request.Id > promisedEpochId)
            {
                Console.WriteLine($"[{nodeId}] PrepareRequest accepted from {request.Id % nodes.Count()}");
                promisedEpochId = request.Id; // overrides the last leader promised epoch id
                PrepareReply prepareReply = new PrepareReply { Id = promisedEpochId };

                if (acceptedValue is not null)
                    prepareReply.AcceptedValue = acceptedValue;

                return Task.FromResult(prepareReply);
            }
        }

        Console.WriteLine($"[{nodeId}] Prepare request rejected from {request.Id % nodes.Count()}. Current promise {promisedEpochId}.");
        return Task.FromResult(new PrepareReply
        {
            Id = promisedEpochId
        });
    }

    public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    {
        Console.WriteLine($"[{nodeId}] Accept request received from {request.AcceptedValue.Id % nodes.Count()}, AcceptId: {request.AcceptedValue.Id}.");

        // Already promised to another younger leader
        if (request.AcceptedValue.Id < promisedEpochId)
        {
            Console.WriteLine($"[{nodeId}] Accept request rejected from {request.AcceptedValue.Id % nodes.Count()}. Current promise {promisedEpochId}.");
            return Task.FromResult(new AcceptReply
            {
                Id = promisedEpochId
            });
        }

        lock (lockAcceptedValue)
        {
            // Overrides the current value with the one from the acceptor
            if (acceptedValue is null || acceptedValue.Id < request.AcceptedValue.Id)
            {
                accepted = 1;
                acceptedValue = request.AcceptedValue;
                Console.WriteLine($"[{nodeId}] Accept request accepted from {request.AcceptedValue.Id % nodes.Count()} with {acceptedValue}.");
                acceptedTokenSource.Cancel();
                acceptedTokenSource = new();
                acceptedCt = acceptedTokenSource.Token;
                Task.Run(() => BroadcastAcceptedRequest(), acceptedCt);
            }
            else if (acceptedValue.Id == request.AcceptedValue.Id)
            {
                // Delayed leader accept
                Console.WriteLine($"[{nodeId}] Accept request (delayed) accepted from {request.AcceptedValue.Id % nodes.Count()} with {acceptedValue}.");
                accepted++;
                Task.Run(() => BroadcastAcceptedRequest(), acceptedCt);
            }
        }
        return Task.FromResult(new AcceptReply
        {
            Id = promisedEpochId
        });
    }


    public override Task<Empty> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        Console.WriteLine($"[{nodeId}] AcceptedRequest received from {request.Id % nodes.Count()} with {acceptedValue}.");

        lock (lockAcceptedValue)
        {
            // Overrides the current value with the one from the acceptor
            if (acceptedValue is null || acceptedValue.Id < request.AcceptedValue.Id)
            {
                acceptedValue = request.AcceptedValue;
                accepted = 1;
            }

            Console.WriteLine($"[{nodeId} Accepted before {accepted}");
            accepted++;
            Console.WriteLine($"[{nodeId} Accepted after {accepted} {accepted == QUORUM_SIZE}");

            // end of paxos instance (Decide)
            if (accepted == QUORUM_SIZE)
            {
                Console.WriteLine($"[{nodeId}] Quorum reached on {acceptedValue}");
                var response = new LeasesResponse();
                response.EpochId = currentEpoch;
                response.Leases.Add(acceptedValue.Leases);
                leaseManagerService.Send(response);
            }
        }
        return Task.FromResult(new Empty());
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

    private void BroadcastPrepareRequest(CancellationTokenSource tokenSource, CancellationToken token)
    {
        Console.WriteLine($"[{nodeId}] Prepare BroadCast Started.");

        var prepareRequest = new PrepareRequest
        {
            Epoch = currentEpoch,
            Id = currentEpochId,
        };
        var broadcastLock = new object();
        int count = 1;

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
                var token_copy = token;
                int requestTries = 1;
                while (true) // loops while there is an exception
                {
                    try
                    {
                        Console.WriteLine($"[{nodeId}] Broadcasting PrepareRequest to node {idAux}");
                        var client = new Paxos.PaxosClient(nodes[idAux]);
                        var reply = await client.PrepareAsync(prepareRequest);
                        Console.WriteLine($"[{nodeId}] PrepareReply received from node {idAux}");

                        if (reply.Id > currentEpochId)
                        {
                            Console.WriteLine($"[{nodeId}] Current Epoch is outdated. Cancelling Prepare Broadcast.");

                            // Restart BroadcastPrepareRequest
                            CalculateNewCurrentEpochId(reply.Id);
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
                                    Console.WriteLine($"[{nodeId}] Received PrepareReply from {idAux} with value {acceptedValue}.");
                                }
                            }
                        }

                        // Double lock mechanism to prevent multiple BroadCastAcceptRequests
                        if (count < QUORUM_SIZE)
                        {
                            lock (broadcastLock)
                            {
                                count++;
                                if (count == QUORUM_SIZE)
                                    Task.Run(() => BroadcastAcceptRequest(), token);
                            }
                        }
                        break; // exits the while loop
                    }
                    catch (Exception e)
                    {
                        // if (token.)
                        await BroadcastExceptionHandler(idAux, requestTries);
                        requestTries++;
                    }
                }
            }, token);
        }
    }

    /**
        Broadcasts the AcceptRequest to all nodes.
        Increments the accepted value because the proposer accepts its own value.
    */
    private void BroadcastAcceptRequest()
    {
        // generate local value if necessary
        if (acceptedValue is null)
            GenerateNewValue();

        accepted = 1;
        AcceptRequest acceptRequest = new AcceptRequest
        {
            AcceptedValue = acceptedValue
        };

        Console.WriteLine($"[{nodeId}] Broadcasting AcceptRequest with {acceptRequest.AcceptedValue}.");
        for (int id = 0; id < nodes.Count(); id++)
        {
            if (id == nodeId)
                continue;

            var idAux = id;
            Task.Run(async () =>
            {
                int requestTries = 1;
                while (true) // loops while there is an exception
                {
                    try
                    {
                        Console.WriteLine($"[{nodeId}] Sending AcceptRequest to {idAux} with {acceptRequest.AcceptedValue}.");
                        GrpcChannel channel = nodes[idAux];
                        var client = new Paxos.PaxosClient(channel);
                        AcceptReply reply = await client.AcceptAsync(acceptRequest);

                        if (reply.Id > currentEpochId)
                        {
                            Console.WriteLine($"[{nodeId}] Current Epoch is outdated. Cancelling Accept Broadcast.");
                            CalculateNewCurrentEpochId(reply.Id);
                            tokenSource.Cancel();
                            return;
                        }
                        break; // exits the while loop
                    }
                    catch (Exception e)
                    {
                        await BroadcastExceptionHandler(idAux, requestTries);
                        requestTries++;
                    }
                }
            }, ct);
        }
    }


    private void BroadcastAcceptedRequest()
    {
        AcceptedRequest acceptedRequest = new AcceptedRequest
        {
            Id = nodeId,
            AcceptedValue = acceptedValue,
        };
        for (int id = 0; id < nodes.Count(); id++)
        {
            var idAux = id;
            Task.Run(async () =>
            {
                int requestTries = 1;
                while (true) // loops while there is an exception
                {
                    try
                    {
                        Console.WriteLine($"[{nodeId}] Sending AcceptedRequest to {idAux} with {acceptedRequest.AcceptedValue}.");
                        GrpcChannel channel = nodes[idAux];
                        var client = new Paxos.PaxosClient(channel);
                        await client.AcceptedAsync(acceptedRequest);
                        break; // exits the while loop
                    }
                    catch (Exception e)
                    {
                        await BroadcastExceptionHandler(idAux, requestTries);
                        requestTries++;
                    }
                }
            }, acceptedCt);
        }
    }

    /**
        Prints exception message, awaits for backoffTimeout and returns the new backoffTimeout.
    */
    async private Task BroadcastExceptionHandler(int id, int tries)
    {
        Console.WriteLine($"[{nodeId}] Trying to resend to: {id} [{tries}]");
        await Task.Delay(tries * 2 * 1000);
    }

    /**
        Generates a new value for the current epoch.
        The value is generated based on the requests received.
        The previous Leases are retained. New Leases are generated based on the requests.
        This also cleans the requests list.
*/
    private void GenerateNewValue()
    {
        AcceptedValue newAcceptedValue = new();
        List<Lease> leases = new List<Lease>();
        List<Lease> leaseRequests = leaseManagerService.GetLeaseRequests();
        HashSet<string> assignedLeaseIds = new();
        foreach (var leaseRequest in leaseRequests)
        {
            if (leaseRequest.RequestIds.ToList().TrueForAll(id => !assignedLeaseIds.Contains(id)))
            {
                assignedLeaseIds.UnionWith(leaseRequest.RequestIds);
                leases.Add(leaseRequest);
            }
        }
        newAcceptedValue.Leases.Add(leases);
        newAcceptedValue.Id = currentEpochId;

        if (acceptedValue is null)
        {
            lock (lockAcceptedValue)
            {
                if (acceptedValue is null)
                    acceptedValue = newAcceptedValue;
            }
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

    private void CalculateNewCurrentEpochId(int minimumId)
    {
        if (currentEpochId < minimumId)
        {
            lock (lockCurrentEpochId)
            {
                while (currentEpochId < minimumId)
                {
                    currentEpochId += nodes.Count();
                }
            }
        }
    }

    // ------------------------------------------------ NOT USED ------------------------------------------------
    private GrpcChannel GetNodeChannel(int id)
    {
        return nodes[id % nodes.Count()];
    }

    /**
        Overrides the paxos chosen value 
*/
    // private void DefineNewValue(List<GrpcPaxos.Lease> leases, int writeTimestamp, int readTimestamp)
    // {
    //     List<LeaseRequest> leaseRequests = new List<LeaseRequest>();
    //     foreach (GrpcPaxos.Lease lease in leases)
    //     {
    //         leaseRequests.Add(new LeaseRequest(lease.TransactionManager, lease.Permissions.ToHashSet()));
    //     }
    //     LeaseAtributionOrder chosenOrder = new LeaseAtributionOrder();
    //
    //     foreach (LeaseRequest leaseRequest in leaseRequests)
    //     {
    //         if (chosenOrder.Contains(leaseRequest))
    //         {
    //             int maxEpoch = chosenOrder.GetGreatestAssignedLeaseEpoch(leaseRequest); // should be =/= -1
    //             chosenOrder.AddLease(maxEpoch, leaseRequest);
    //         }
    //         else
    //         {
    //             chosenOrder.AddLease(currentEpoch, leaseRequest);
    //         }
    //     }
    //     //leaseManagerService.proposedValueAndTimestamp = new ProposedValueAndTimestamp(chosenOrder, writeTimestamp, readTimestamp);
    // }

    // FIXME: adapt to new lease manager
    private string GenerateLeases2()
    {
        return $"Node: {nodeId}";
    }
}
