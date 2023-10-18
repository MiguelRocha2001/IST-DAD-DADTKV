namespace utils;

using System.Diagnostics;

enum ProcessState { NORMAL, CRASHED }

class Utils
{
    public static void KillProcesses(HashSet<Process> processes)
    {
        Console.WriteLine("Killing all processes!");
        foreach (Process process in processes)
        {
            process.Kill();
        }
        Console.WriteLine("Processes killed!");
    }

    static string GetTransactionManagerUrlsArgument(List<String> transactionManagersUrls)
    {
        string transactionManagersArg = "";
        foreach (string transactionManagerUrl in transactionManagersUrls)
        {
            transactionManagersArg += transactionManagerUrl + ",";
        }
        transactionManagersArg = transactionManagersArg.Trim(','); // removes last comma
        transactionManagersArg += "";

        return transactionManagersArg;
    }

    static public string BuildLeaseManagerArguments(
        string nodeId, 
        string host,
        string port,
        List<String> transactionManagersUrls,
        List<String> leaseManagersUrls,
        int timeSlots,
        string starts,
        int lasts
    ) 
    {
        string nodeIdArg = nodeId.Last().ToString();
        string leaseManagersArg = "";
        foreach (string leaseManagerUrl in leaseManagersUrls)
        {
            leaseManagersArg += leaseManagerUrl + ",";
        }
        leaseManagersArg = leaseManagersArg.Trim(','); // removes last comma
        leaseManagersArg += "";

        string t = nodeIdArg + " " + host + " " + port + " " + leaseManagersArg + " " + GetTransactionManagerUrlsArgument(transactionManagersUrls) + " " + timeSlots + " " + starts + " " + lasts;
        return t;
    }

    static public string BuildTransactionManagerArguments(
        string nodeId, 
        string host,
        string port,
        int processIndex,
        List<String> transactionManagersUrls,
        List<String> leaseManagersUrls,
        int timeSlots,
        string starts,
        int lasts,
        Dictionary<int, List<ProcessState>> processesState
    )
    {
        string nodeIdArg = nodeId.Last().ToString();
        int quorumSize = leaseManagersUrls.Count;
        return nodeIdArg + " " + host + " " + port + " " + GetTransactionManagerUrlsArgument(transactionManagersUrls) + " " + quorumSize + " " + timeSlots + " " + starts + " " + lasts + " " + BuildProcessStateArgument(processIndex, processesState);
    }

    static string BuildProcessStateArgument(
        int processIndex,
        Dictionary<int, List<ProcessState>> processesState
    )
    {
        string processStateArg = "[";
        foreach (KeyValuePair<int, List<ProcessState>> entry in processesState)
        {
            string state = entry.Key + (entry.Value[processIndex] == ProcessState.NORMAL ? "N" : "C");
            processStateArg += state + ",";
        }
        processStateArg = processStateArg.Trim(','); // removes last comma
        processStateArg += "]";
        Console.WriteLine("processStateArg: " + processStateArg);
        return processStateArg;
    }
}