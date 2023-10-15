namespace utils;

using Grpc.Net.Client;

public enum ProcessState { NORMAL, CRASHED }

class Utils {

    public static List<(int, string)> FromStringToNodes(string urls)
    {
        urls = urls.Trim('[');
        urls = urls.Trim(']');

        List<(int, string)> nodes = new List<(int, string)>();
        string[] leaseManagersUrlsAux = urls.Split(',');
        foreach(string serverUlr in leaseManagersUrlsAux )
        {
            string[] split = serverUlr.Split(':');
            nodes.Add((int.Parse(split[2]), serverUlr));
        }
        return nodes;
    }

    public static GrpcChannel[] GetChannels(List<(int, string)> nodes)
    {
        GrpcChannel[] channels = new GrpcChannel[nodes.Count];
        int i = 0;
        foreach (var node in nodes)
        {
            channels[i] = GrpcChannel.ForAddress(node.Item2);
            i++;
        }
        return channels;
    }

    public static DateTime? FromStringToDateTime(string starts)
    {
        DateTime now = DateTime.Now;
        string[] split = starts.Split(':');
        if (split.Length != 3)
        {
            return null;
        }
        int hour = int.Parse(split[0]);
        int minute = int.Parse(split[1]);
        int second = int.Parse(split[2]);
        return new DateTime(now.Year, now.Month, now.Day, hour, minute, second);
    }

    public static List<ProcessState> FromStringToProcessStateList(string processStateArg)
    {
        List<ProcessState> processStates = new List<ProcessState>();
        processStateArg = processStateArg.Trim('[');
        processStateArg = processStateArg.Trim(']');
        string[] split = processStateArg.Split(',');
        foreach (string state in split)
        {
            processStates.Add(state == "N" ? ProcessState.NORMAL : ProcessState.CRASHED);
        }
        return processStates;
    }

    /**
        Returns the seconds between two dates.
        [startTime] shhouls be greater than [now].
        @param startTime The start date.
        @param now The end date.
    */
    public static int GetSecondsApart(DateTime startTime, DateTime now)
    {
        TimeSpan timeSpan = startTime - now;
        int totalSeconds = (int)timeSpan.TotalSeconds;
        if (totalSeconds < 0)
            throw new Exception("startTime is in the past!");
        return totalSeconds;
    }
}