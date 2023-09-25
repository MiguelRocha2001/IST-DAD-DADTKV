namespace LeaseManager.Services;

using Grpc.Core;
using GrpcPaxos;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;

public class PaxosService : Paxos.PaxosBase
{
    // const string clientScriptFilename = "../configuration_sample";
    // int timeSlot;
    // int nOfSlots;
    // // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    // List<RequestLeaseRequest> requests = new List<RequestLeaseRequest>();
    // Boolean isLeader = false;
    // string node;
    // HashSet<string> nodes;
    // int promised = 0; // FIXME: use concurrency bullet proof
    // int accepted = 0; // FIXME: use concurrency bullet proof
    // int epoch = 0; // FIXME: use concurrency bullet proof
    // HashSet<Tuple<RequestLeaseRequest, int>> value; // FIXME: use concurrency bullet proof
    // private Monitor monitor; // monitor to be used by the threads that are waiting for the paxos instance to end
    //
    // private class Lease
    // {
    //     public HashSet<string> permissions;
    //
    //     public Lease(HashSet<string> permissions)
    //     {
    //         this.permissions = permissions;
    //     }
    // }
    //
    //
    public PaxosService(IConfiguration configuration)
    {
        Console.WriteLine("FDSSSS");
        int id = int.Parse(configuration["id"]);
        Console.WriteLine(id);

         //ProcessConfigurationFile();
    //
    //     void BroadcastPrepareRequest()
    //     {
    //         foreach (string node in nodes)
    //         {
    //             using var channel = GrpcChannel.ForAddress("http://localhost:5000");
    //             var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
    //             client.Prepare(new PrepareRequest());
    //         }
    //     }
    //
    //     this.node = node;
    //     this.nodes = nodes;
    //     DefineLeader();
    //     if (isLeader)
    //     {
    //         BroadcastPrepareRequest();
    //     }
    }
    //
    // void ProcessConfigurationFile()
    // {
    //     IEnumerator<string> lines = File.ReadLines(clientScriptFilename).GetEnumerator();
    //     while (lines.MoveNext())
    //     {
    //         string line = lines.Current;
    //         if (!line.StartsWith('#')) // not a comment
    //         {
    //             string[] split = line.Split(' ');
    //             string firstToken = split[1];
    //             /*
    //             if (line.StartsWith('P'))
    //             {
    //                 string type = split[2];
    //                 if (type == "L")
    //                 {
    //                     string name = firstToken;
    //                     int href = int.Parse(split[3]);
    //                     // TODO: create a new lease manager
    //                 }
    //             }
    //             */
    //             if(line.StartsWith('D'))
    //             {
    //                 timeSlot = int.Parse(firstToken);
    //             }
    //             else if(line.StartsWith('S'))
    //             {
    //                 nOfSlots = int.Parse(firstToken);
    //             }
    //         }
    //     }
    // }
    //
    // void DefineLeader()
    // {
    //     IEnumerable<int> nodesCastedToInt = nodes.Cast<int>(); // cast the nodes to int
    //     int min = nodesCastedToInt.Min(); // get the min value
    //     isLeader = min == int.Parse(node);
    // }
    //
    // public override Task<RequestLeaseReply> RequestLease(RequestLeaseRequest request, ServerCallContext context)
    // {
    //     Console.WriteLine($"Request: {request.TransactionManager}");
    //
    //     requests.Add(request);
    //     monitor.Wait(); // wait until the paxos instance is executed
    //     
    //     // decided value is now available
    //     RequestLeaseReply reply = new RequestLeaseReply();
    //     reply.Permissions.Add(value);
    //
    //     return Task.FromResult();
    // }
    //
    //
    // public override Task<PrepareReply> Prepare(PrepareRequest request, ServerCallContext context)
    // {
    //     void BroadcastAcceptRequest()
    //     {
    //         foreach (string node in nodes)
    //         {
    //             using var channel = GrpcChannel.ForAddress("http://localhost:5000");
    //             var client = new DadTkvLeaseManagerService.DadTkvLeaseManagerServiceClient(channel);
    //             client.Accept(new AcceptRequest());
    //         }
    //     }
    //
    //     promised++; // one more promise accepted
    //     if (promised > nodes.Count / 2) // majority
    //     {
    //         BroadcastAcceptRequest();
    //     }
    //     return Task.FromResult(new PrepareReply{});
    // }
    //
    // public override Task<PromiseReply> Promise(PromiseRequest request, ServerCallContext context)
    // {
    //     return Task.FromResult(new PromiseReply{});
    // }
    //
    // public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
    // {
    //     return Task.FromResult(new AcceptReply{});
    // }
    //
    // private void DefineNewValue()
    // {
    //     value = requests; // FIXME: maybe use a better sorting rule
    // }
    //
    // public override Task<AcceptedReply> Accepted(AcceptedRequest request, ServerCallContext context)
    // {
    //     accepted++; // one more accept received
    //     if (accepted > nodes.Count / 2) // majority
    //     {
    //         // end of paxos instance (Decide)
    //         // wake up the thread that is waiting for the paxos instance to end (the one that is waiting for the lease request to be executed)
    //         monitor.PulseAll();
    //
    //         DefineNewValue();
    //     }
    //     return Task.FromResult(new AcceptedReply{});
    // }
}
