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
    string previousLeader;
    string node;
    HashSet<string> nodes;
    int promised = 0; // FIXME: use concurrency bullet proof
    int accepted = 0; // FIXME: use concurrency bullet proof
    int epoch = 0; // FIXME: use concurrency bullet proof
    IPaxosStep step;
    
    LeaseAtributionOrder value;


    
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
            Console.WriteLine("Broadcasting PrepareRequest");
            foreach (string node in nodes)
            {
                if (!node.Equals(this.node))
                {
                    Console.WriteLine("Node: " + node);
                    Console.WriteLine("This node: " + this.node);
                    Console.WriteLine("Sending PrepareRequest to " + node);
                    using var channel = GrpcChannel.ForAddress(node);
                    var client = new Paxos.PaxosClient(channel);
                    client.Prepare(new PrepareRequest());
                }
            }
        }

        void DefineLeader()
        {
            int lowerPort = -1;
            string leaderNode = "";
            foreach (string node in nodes)
            {
                string port = node.Split(':')[2];
                int portInt = int.Parse(port);
                if (lowerPort == -1 || portInt < lowerPort)
                {
                    lowerPort = portInt;
                    leaderNode = node;
                    break;
                }
            }
            isLeader = leaderNode == node;
        }

        DefineLeader();
        Thread.Sleep(2000); // wall time (make it configurable in the future)
        if (isLeader && previousLeader != null)
        {
            BroadcastPrepareRequest();
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
        void BroadcastAcceptRequest()
        {
            foreach (string node in nodes)
            {
                using var channel = GrpcChannel.ForAddress("http://localhost:5000");
                var client = new Paxos.PaxosClient(channel);
                client.Accept(new AcceptRequest());
            }
        }

        Console.WriteLine("Prepare request received");

        promised++; // one more promise accepted
        if (promised > nodes.Count / 2) // majority
        {
            BroadcastAcceptRequest();
        }
        return Task.FromResult(new PrepareReply{});
    }

    public override Task<PromiseReply> Promise(PromiseRequest request, ServerCallContext context)
    {
        Console.WriteLine("Promise request received");
        if (request.Epoch > epoch)
        {
            step = new Step1(request.epoch, request.value);
        }

        return Task.FromResult(new PromiseReply{});
    }

    public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    {
        Console.WriteLine("Accept request received");
        return Task.FromResult(new AcceptReply{});
    }

    private void DefineNewValue()
    {
        List<RequestLeaseRequest> requests = leaseManagerService.requests;

        HashSet<Tuple<string, Lease>> leases = new HashSet<Tuple<string, Lease>>();
        value = new LeaseAtributionOrder(leases);
    }

    public override Task<AcceptedReply> Accepted(AcceptedRequest request, ServerCallContext context)
    {
        accepted++; // one more accept received
        if (accepted > nodes.Count / 2) // majority
        {
            // end of paxos instance (Decide)
            // wake up the thread that is waiting for the paxos instance to end (the one that is waiting for the lease request to be executed)
            //monitor.PulseAll();

            DefineNewValue();
        }
        return Task.FromResult(new AcceptedReply{});
    }
}
