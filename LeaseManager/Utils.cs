using Grpc.Net.Client;
using LeaseManager.Services;

namespace utils;

class Utils
{
    static public List<(int, string)> FromStringToNodes(string leaseManagerUrls)
    {
        List<(int, string)> nodes = new List<(int, string)>();

        leaseManagerUrls = leaseManagerUrls.Trim('[');
        leaseManagerUrls = leaseManagerUrls.Trim(']');

        if (leaseManagerUrls == "")
            return nodes;

        string[] leaseManagersUrlsAux = leaseManagerUrls.Split(',');
        foreach(string serverUlr in leaseManagersUrlsAux )
        {
            string[] split = serverUlr.Split(':');
            nodes.Add((int.Parse(split[2]), serverUlr));
        }
        return nodes;
    }

    static public GrpcChannel[] GetChannels(string[] nodes)
    {
        GrpcChannel[] channels = new GrpcChannel[nodes.Length];
        int i = 0;
        foreach (var node in nodes)
        {
            channels[i] = GrpcChannel.ForAddress(node);
            i++;
        }
        return channels;
    }

    static public DateTime FromStringToDateTime(string starts)
    {
        DateTime now = DateTime.Now;
        string[] split = starts.Split(':');
        int hour = int.Parse(split[0]);
        int minute = int.Parse(split[1]);
        int second = int.Parse(split[2]);
        return new DateTime(now.Year, now.Month, now.Day, hour, minute, second);
    }

    /**
        Returns the seconds between two dates.
        [startTime] shhouls be greater than [now].
        @param startTime The start date.
        @param now The end date.
    */
    static public int GetSecondsApart(DateTime startTime, DateTime now)
    {
        TimeSpan timeSpan = startTime - now;
        int totalSeconds = (int)timeSpan.TotalSeconds;
        if (totalSeconds < 0)
            throw new Exception("startTime is in the past!");
        return totalSeconds;
    }

    public static ServerState[] FromStringToServerState(string statesStr)
    {
        statesStr = statesStr.Trim('[');
        statesStr = statesStr.Trim(']');

        ServerState[] state = {};

        int index = 0;
        foreach(string stateAux in statesStr.Split(","))
        {
            int epoch = int.Parse(stateAux[0].ToString());
            if (epoch == index)
            {
                ServerState serverState1 = stateAux[1] == 'N' ? ServerState.Normal : ServerState.Crashed;
                state = state.Append(serverState1).ToArray();
                index++;
            }
            else
            {
                ServerState serverState1 = state[index-1];
                for(int i = 0, times = epoch - index; i < times; i++)
                {
                    state = state.Append(serverState1).ToArray();
                    index++;                    
                }
                ServerState serverState2 = stateAux[1] == 'N' ? ServerState.Normal : ServerState.Crashed;
                state = state.Append(serverState2).ToArray();
            }
        }
        return state;
    }
}