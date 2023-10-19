namespace LeaseManager.Services;

using Grpc.Core;
using GrpcLeaseService;
using Grpc.Net.Client;
using System.Threading.Tasks;

public class LeaseManagerService : LeaseService.LeaseServiceBase
{
    List<Lease> requests = new();
    GrpcChannel[] nodes;
    int nodeId;

    public LeaseManagerService(int nodeId, GrpcChannel[] nodes)
    {
        this.nodes = nodes;
        this.nodeId = nodeId;
    }

    public override Task<Empty> RequestLease(Lease request, ServerCallContext context)
    {
        Console.WriteLine($"Request: {request.TransactionManagerId}");
        Console.WriteLine($"Request: {request.RequestIds}");

        lock (this)
        {
            requests.Add(request);
        }
        return Task.FromResult(new Empty());
    }

    public List<Lease> GetLeaseRequests()
    {
        lock (this)
        {
            List<Lease> requestsCopy = new List<Lease>(requests.Select(value => value.Clone()));
            requests.RemoveAll(value => true);
            return requestsCopy;
        }
    }

    
    public void Send(LeasesResponse response, CancellationToken ct)
    {
        var epoch = response.EpochId;
        for (int id = 0; id < nodes.Count(); id++)
        {
            var idAux = id;
            Task.Run(async () =>
            {
                int requestTries = 1;
                while (true)
                {
                    try
                    {
                        var client = new LeaseService.LeaseServiceClient(nodes[idAux]);
                        client.SendLeases(response);
                        break;
                    }
                    catch (Exception)
                    {
                        if (ct.IsCancellationRequested){
                            break;
                        }
                        await BroadcastExceptionHandler(nodeId, requestTries, epoch);
                        requestTries++;
                    }
                }
            }, ct);
        }
    }

    /**
        Prints exception message, awaits for backoffTimeout and returns the new backoffTimeout.
    */
    async private Task BroadcastExceptionHandler(int id, int tries, int epoch)
    {
        Console.WriteLine($"[{nodeId}] Trying to resend Leases to TransactionManager. Epoch: {epoch}, Tries: {tries}");
        await Task.Delay(tries * 2 * 1000);
    }
}
