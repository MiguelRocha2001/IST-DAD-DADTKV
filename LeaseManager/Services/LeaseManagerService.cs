namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class LeaseManagerService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    Paxos paxos = new Paxos();
    List<RequestLeaseRequest> requests = new List<RequestLeaseRequest>();
    Boolean isLeader = false;
    int promised = 0; // FIXME: use concurrency bullet proof

    public LeaseManagerService(string node, HashSet<string> nodes)
    {
        void BroadcastPrepareRequest()
        {
            foreach (string node in nodes)
            {
                using var channel = GrpcChannel.ForAddress("http://localhost:5000");
                var client = new DADTKV.DADTKVClient(channel);
                client.Prepare(new PrepareRequest());
            }
        }

        this.node = node;
        this.nodes = nodes;
        DefineLeader();
        if (isLeader)
        {
            BroadcastPrepareRequest();
        }
    }

    public void DefineLeader()
    {
        IEnumerable<int> nodesCastedToInt = nodes.Cast<int>(); // cast the nodes to int
        int min = nodesCastedToInt.Min(); // get the min value
        isLeader = min == int.Parse(node);
    }

    public override Task<RequestLeaseReply> RequestLease(RequestLeaseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        
        // TODO: store this request and delay until next paxos instance is executed
        // maybe use a pool request
        // maybe put this thread to sleep and wake it up when the paxos instance is executed

        return Task.FromResult(new RequestLeaseReply{});
    }


    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        void BroadcastAcceptRequest()
        {
            foreach (string node in nodes)
            {
                using var channel = GrpcChannel.ForAddress("http://localhost:5000");
                var client = new DADTKV.DADTKVClient(channel);
                client.Accept(new AcceptRequest());
            }
        }

        Console.WriteLine($"Request: {request.TransactionManager}");
        promised++; // one more promise accepted
        if (promised > nodes.Count / 2) // majority
        {
            BroadcastAcceptRequest();
        }
    }

    public override Task<AcceptReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        return Task.FromResult(new RequestLeaseReply{});
    }

    public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        return Task.FromResult(new RequestLeaseReply{});
    }

    public override Task<AcceptReply> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManager}");
        return Task.FromResult(new RequestLeaseReply{});
    }
}