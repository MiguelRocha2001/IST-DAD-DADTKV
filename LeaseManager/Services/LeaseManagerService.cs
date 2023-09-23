namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;
using Grpc.Net.Client;
using System.Diagnostics;

public class LeaseManagerService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    const string clientScriptFilename = "../configuration_sample";
    int timeSlot;
    int nOfSlots;
    // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    List<RequestLeaseRequest> requests = new List<RequestLeaseRequest>();
    Boolean isLeader = false;
    string node;
    HashSet<string> nodes;
    int promised = 0; // FIXME: use concurrency bullet proof

    public LeaseManagerService(string node, HashSet<string> nodes)
    {
        ProcessConfigurationFile();

        void BroadcastPrepareRequest()
        {
            foreach (string node in nodes)
            {
                using var channel = GrpcChannel.ForAddress("http://localhost:5000");
                var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
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

    void DefineLeader()
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

        requests.Add(request);

        return Task.FromResult(new RequestLeaseReply{});
    }


    public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    {
        void BroadcastAcceptRequest()
        {
            foreach (string node in nodes)
            {
                using var channel = GrpcChannel.ForAddress("http://localhost:5000");
                var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
                client.Accept(new AcceptRequest());
            }
        }

        promised++; // one more promise accepted
        if (promised > nodes.Count / 2) // majority
        {
            BroadcastAcceptRequest();
        }
        return Task.FromResult(new PrepareReply{});
    }

    public override Task<PromiseReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        return Task.FromResult(new PromiseReply{});
    }

    public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    {
        return Task.FromResult(new AcceptReply{});
    }

    public override Task<AcceptedReply> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        return Task.FromResult(new AcceptedReply{});
    }
}