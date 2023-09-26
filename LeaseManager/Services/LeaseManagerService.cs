namespace LeaseManager.Services;

using Grpc.Core;
using GrpcDADTKV;
using Grpc.Net.Client;
using System.Threading;
using System.Threading.Tasks;
using GrpcPaxos;

public class LeaseManagerService : DadTkvLeaseManagerService.DadTkvLeaseManagerServiceBase
{
    const string clientScriptFilename = "../configuration_sample";
    int timeSlot;
    int nOfSlots;
    // const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/configuration_sample";
    public List<RequestLeaseRequest> requests = new List<RequestLeaseRequest>();
    Boolean isLeader = false;
    string node;
    HashSet<string> nodes;
    HashSet<Tuple<RequestLeaseRequest, int>> value; // FIXME: use concurrency bullet proof
    //Monitor monitor; // monitor to be used by the threads that are waiting for the paxos instance to end
    private class Lease
    {
        public HashSet<string> permissions;
            public Lease(HashSet<string> permissions)
        {
            this.permissions = permissions;
        }
    }
    public LeaseManagerService(string node, HashSet<string> nodes)
    {
        
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
        requests.Add(request);
        Monitor.Wait(this); // wait until the paxos instance is executed
        
        // decided value is now available
        RequestLeaseReply reply = new RequestLeaseReply();
        //reply.Permissions.Add(value);
        return Task.FromResult(reply);
    }
}
