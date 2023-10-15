namespace LeaseManager.Services;

using Grpc.Core;
using GrpcPaxos;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using AcceptedValue = GrpcPaxos.AcceptedValue;
using GrpcLeaseService;
using System.Collections;
using System.Collections.Concurrent;

/*
class ListLeaseComparator : EqualityComparer<List<Lease>>
{
    public override bool Equals(List<Lease>? l1, List<Lease>? l2) =>
        StructuralComparisons.StructuralEqualityComparer.Equals(l1?.ToArray(), l2?.ToArray());

    public override int GetHashCode(List<Lease> l) =>
        StructuralComparisons.StructuralEqualityComparer.GetHashCode(l.ToArray());
}
*/

class ListLeaseOrderComparator : EqualityComparer<List<LeaseOrder>>
{
    public override bool Equals(List<LeaseOrder>? l1, List<LeaseOrder>? l2) =>
        StructuralComparisons.StructuralEqualityComparer.Equals(l1?.ToArray(), l2?.ToArray());

    public override int GetHashCode(List<LeaseOrder> l) =>
        StructuralComparisons.StructuralEqualityComparer.GetHashCode(l.ToArray());
}




public class PaxosService : Paxos.PaxosBase
{
    LeaseManagerService leaseManagerService;
    const string clientScriptFilename = "../configuration_sample";
    readonly int QUORUM_SIZE;
    int timeSlots; // TODO: use this
    int slotTime;
    int nodeId;
    GrpcChannel[] nodes;
    int currentEpoch = 1;
    int currentEpochId;
    int promisedEpochId = -1;

    // Locks
    object lockCurrentEpochId = new object();
    object lockPrepareMethod = new object();
    object lockAcceptMethod = new object();
    object lockAcceptedMethod = new object();

    //ConcurrentDictionary<List<Lease>, int> acceptedValues = new(new ListLeaseComparator());
    ConcurrentDictionary<List<LeaseOrder>, int> acceptedValues = new(new ListLeaseOrderComparator());
    AcceptedValue? highestAcceptedValue = null;

    public PaxosService(int nodeId, GrpcChannel[] nodes, LeaseManagerService leaseManagerService, int timeSlots, int slotTime)
    {
        Console.WriteLine(nodeId);

        this.QUORUM_SIZE = nodes.Count() / 2 + 1;
        Console.WriteLine(QUORUM_SIZE);
        this.nodeId = nodeId;
        this.nodes = nodes;
        this.leaseManagerService = leaseManagerService;
        this.currentEpochId = nodeId;
        this.timeSlots = timeSlots;
        this.slotTime = slotTime;
    }

    public async void Init()
    {

        DateTime? startEpochTime = null;
        CancellationTokenSource tokenSource = new();
        CancellationToken ct = tokenSource.Token;
        //TimeSpan epochTimeInterval = TimeSpan.FromSeconds(10);
        TimeSpan epochTimeInterval = TimeSpan.FromSeconds(slotTime);

        void ResetProperties()
        {
            currentEpochId = nodeId;
            promisedEpochId = -1;
            currentEpoch++;
            startEpochTime = null;
            acceptedValues.Clear();
            highestAcceptedValue = null;
        }

        while (true)
        {
            startEpochTime ??= DateTime.Now;
            try
            {
                await Task.Run(async () =>
                {
                    Console.WriteLine($"[{nodeId}] Starting new paxos instance.");
                    if (IsLeader())
                    {
                        Console.WriteLine($"[{nodeId}] I am the leader.");
                        BroadcastPrepareRequest(tokenSource, ct);
                    }

                    // No need to cancel because it is assumed that the state only changes with
                    // each epoch.
                    TimeSpan delay = (epochTimeInterval - (DateTime.Now - startEpochTime)) ?? TimeSpan.MaxValue;

                    Console.WriteLine($"[{nodeId}] Waiting {delay} till next epoch.");
                    await Task.Delay(delay, ct);

                    // Paxos was successfull calculate state
                    ResetProperties();
                }, ct);

            }
            catch (Exception)
            {
                TimeSpan delay = epochTimeInterval - (DateTime.Now - startEpochTime) ?? TimeSpan.MaxValue;

                // No time to retry broadcast
                if (delay < TimeSpan.FromMilliseconds(2000))
                {
                    Console.WriteLine($"[{nodeId}] Paxos was cancelled. No time to try again {delay.TotalSeconds} < 2 Seconds.");
                    ResetProperties();
                    await Task.Delay(delay);
                }
                // Give time for other lease Managers to achieve progress
                else
                {
                    Console.WriteLine($"[{nodeId}] Paxos was cancelled. Retrying in 2 seconds.");
                    await Task.Delay(2000);
                }
            }
            tokenSource.Cancel();
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

    private bool IsSuspect(int id) => id != 0;

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

                if (highestAcceptedValue is not null)
                    prepareReply.AcceptedValue = highestAcceptedValue;

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


        lock (lockAcceptMethod)
        {
            if (highestAcceptedValue is null || highestAcceptedValue.Id < request.AcceptedValue.Id)
                highestAcceptedValue = request.AcceptedValue;

            List<LeaseOrder> leaseOrders = request.AcceptedValue.LeaseOrder.ToList();
            int count = InsertOrIncrementAcceptedValue(leaseOrders);
            SendLeasesOrder(leaseOrders);
            if (count == QUORUM_SIZE)
            {
                Console.WriteLine($"[{nodeId}] Quorum was reached on {request.AcceptedValue}");
                Decide();
            }
            Task.Run(() => BroadcastAcceptedRequest(request.AcceptedValue));
        }
        return Task.FromResult(new AcceptReply
        {
            Id = promisedEpochId
        });
    }


    public override Task<Empty> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        Console.WriteLine($"[{nodeId}] AcceptedRequest received from {request.Id % nodes.Count()} with {request.AcceptedValue}.");
        // TODO: I think there is no need for lock here.
        lock (lockAcceptedMethod)
        {
            int count = InsertOrIncrementAcceptedValue(request.AcceptedValue.LeaseOrder.ToList());
            if (count == QUORUM_SIZE)
            {
                Console.WriteLine($"[{nodeId}] Quorum was reached on {request.AcceptedValue}");
                Decide();
            }
        }
        return Task.FromResult(new Empty());
    }

    private void BroadcastPrepareRequest(CancellationTokenSource tokenSource, CancellationToken ct)
    {
        Console.WriteLine($"[{nodeId}] Prepare BroadCast Started.");

        var prepareRequest = new PrepareRequest
        {
            Epoch = currentEpoch,
            Id = currentEpochId,
        };
        var broadcastLock = new object();
        var acceptedValueLock = new object();
        int acceptedPrepares = 1;
        AcceptedValue? acceptedValue = null;

        for (int id = 0; id < nodes.Count(); id++)
        {
            if (id == nodeId)
                continue;

            int idAux = id; // this is because the [id] variable is not captured by the lambda expression
            Task.Run(async () =>
            {
                // var token = ct;
                Console.WriteLine($"[{nodeId}] Broadcasting PrepareRequest to node {idAux}");
                int requestTries = 1;
                while (true) // loops while there is an exception
                {
                    try
                    {
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

                        if (acceptedPrepares >= QUORUM_SIZE) return; // no more promise msg processing required

                        if (reply.AcceptedValue is not null)
                        {
                            lock (acceptedValueLock)
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
                        if (acceptedPrepares < QUORUM_SIZE)
                        {
                            lock (broadcastLock)
                            {
                                acceptedPrepares++;
                                if (acceptedPrepares == QUORUM_SIZE)
                                {
                                    if (acceptedValue is null)
                                    {
                                        acceptedValue = new();
                                        //var leases = leaseManagerService.GetLeaseRequests();
                                        //InsertOrIncrementAcceptedValue(leases);
                                        List<LeaseOrder> leasesOrder = AtributeLeaseRequestOrder(); // decides the order of the leases
                                        InsertOrIncrementAcceptedValue(leasesOrder);
                                        acceptedValue.LeaseOrder.AddRange(leasesOrder);
                                        SendLeasesOrder(leasesOrder); // sends newly generated leases order to TM nodes
                                    }
                                    acceptedValue.Id = currentEpochId;
                                    Task.Run(() => BroadcastAcceptRequest(acceptedValue, tokenSource, ct), ct);
                                }
                            }
                        }
                        break; // exits the while loop
                    }
                    catch (Exception)
                    {
                        await BroadcastExceptionHandler(idAux, requestTries);
                        if (ct.IsCancellationRequested)
                        {
                            Console.WriteLine($"[{nodeId}] PrepareRequest to node {idAux} was cancelled.");
                            return;
                        }
                        requestTries++;
                    }
                }
            }, ct);
        }
    }

    /**
        Broadcasts the AcceptRequest to all nodes.
        Increments the accepted value because the proposer accepts its own value.
    */
    private void BroadcastAcceptRequest(
        AcceptedValue acceptedValue,
        CancellationTokenSource tokenSource,
        CancellationToken ct)
    {
        // accepted = 1;
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

                        if (reply.Id > acceptRequest.AcceptedValue.Id)
                        {
                            Console.WriteLine($"[{nodeId}] Current Epoch is outdated. Cancelling Accept Broadcast.");
                            CalculateNewCurrentEpochId(reply.Id);
                            tokenSource.Cancel();
                            return;
                        }
                        break; // exits the while loop
                    }
                    catch (Exception)
                    {
                        await BroadcastExceptionHandler(idAux, requestTries);
                        if (ct.IsCancellationRequested)
                        {
                            Console.WriteLine($"[{nodeId}] PrepareRequest to node {idAux} was cancelled.");
                            return;
                        }
                        requestTries++;
                    }
                }
            }, ct);
        }
    }


    private void BroadcastAcceptedRequest(AcceptedValue acceptedValue)
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
                    catch (Exception)
                    {
                        await BroadcastExceptionHandler(idAux, requestTries);
                        requestTries++;
                        // TODO: Register a handler to do this automatically
                        if (requestTries == 5)
                            break;
                    }
                }
            });
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

    private int InsertOrIncrementAcceptedValue(List<LeaseOrder> leasesOrder) =>
        acceptedValues.AddOrUpdate(leasesOrder, 1, (k, v) => v + 1);


    private void Decide()
    {
        LeasesResponse response = new LeasesResponse
        {
            EpochId = currentEpoch,
        };
        response.LeaseOrder.Add(acceptedValues.First(v => v.Value == QUORUM_SIZE).Key);
        //leaseManagerService.Send(response);
    }

    private List<LeaseOrder> AtributeLeaseRequestOrder()
    {
        /**
            Checks if current order has a lease that conflicts with the new lease.
            If not, adds the new lease to the order, as the first element.
            If yes, adds the new lease to the order, after the conflicting lease.
        */
        HashSet<Tuple<int, Lease>> OrderNewLeaseRequest(Lease lease, HashSet<Tuple<int, Lease>> order)
        {
            int orderNumber = 0;
            foreach (string requestId in lease.RequestIds)
            {
                foreach (Tuple<int, Lease> tuple in order)
                {    
                    if (tuple.Item2.RequestIds.Contains(requestId))
                    {
                        if (tuple.Item1 >= orderNumber)
                            orderNumber = tuple.Item1 + 1;
                    }
                }
            }
            order.Add(Tuple.Create(orderNumber, lease));
            return order;
        }

        HashSet<Tuple<int, Lease>> order = new HashSet<Tuple<int, Lease>>();
        List<Lease> leaseRequests = leaseManagerService.GetLeaseRequests();
        foreach (Lease lease in leaseRequests)
        {
            order = OrderNewLeaseRequest(lease, order);
        }
        List<LeaseOrder> leasesOrder = new List<LeaseOrder>();
        foreach (Tuple<int, Lease> tuple in order)
        {
            LeaseOrder leaseOrder = new LeaseOrder();
            leaseOrder.Lease = tuple.Item2;
            leaseOrder.Order = tuple.Item1;
            leasesOrder.Add(leaseOrder);
        }
        return leasesOrder;
    }

    private void SendLeasesOrder(List<LeaseOrder> leasesOrder)
    {
        LeasesResponse response = new LeasesResponse
        {
            EpochId = currentEpoch,
        };
        response.LeaseOrder.AddRange(leasesOrder);
        leaseManagerService.Send(response);
    }


    /*
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
                /*
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
    */

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
}
