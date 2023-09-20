namespace TransactionManager.Services;

using Grpc.Core;
using GrpcDADTKV;

public class TransactionManagerService : DADTKV.DADTKVBase
{
    public override Task<TxSubmitReply> TxSubmit(TxSubmitRequest request, ServerCallContext context)
{
    Console.WriteLine($"Client: {request.Client}");
    
    // Construct the reply message with some sample data
    var reply = new TxSubmitReply();
    reply.Result.Add(new DadInt
    {
        Key = "sample_key",
        Value = 12345
    });

    return Task.FromResult(reply);
}

}
