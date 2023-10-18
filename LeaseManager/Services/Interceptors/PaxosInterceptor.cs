using Grpc.Core;
using Grpc.Core.Interceptors;
using LeaseManager.Services;
using GrpcPaxos;

class PaxosInterceptor : Interceptor
{
    PaxosService state;
    public PaxosInterceptor(PaxosService state)
    {
        this.state = state;
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        // Ignore request
        if (state.GetCurrentServerState() == ServerState.Crashed) 
        {
            throw  new RpcException(Status.DefaultCancelled);
        }

        // If request belongs to another epoch -> Ignore
        if (request is Header req && req.Epoch != state.GetCurrentEpoch()) {
            throw  new RpcException(Status.DefaultCancelled);
        }

        return await continuation(request, context);
    }




}
