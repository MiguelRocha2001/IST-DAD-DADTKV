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

    static string GetLeaseManagerUrlsArgument(List<string> leaseManagerUrls)
    {
        string leaseManagersArg = "";
        foreach (string leaseManagerUrl in leaseManagerUrls)
        {
            leaseManagersArg += leaseManagerUrl + ",";
        }
        leaseManagersArg = leaseManagersArg.Trim(','); // removes last comma
        leaseManagersArg += "";

        return leaseManagersArg;
    }

    static public string BuildLeaseManagerArguments(
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
        return nodeIdArg + " " + host + " " + port + " " + GetLeaseManagerUrlsArgument(leaseManagersUrls) + " " + GetTransactionManagerUrlsArgument(transactionManagersUrls) + " " + timeSlots + " " + starts + " " + lasts + " " + BuildProcessStateArgument(processIndex, processesState);
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
        //int quorumSize = leaseManagersUrls.Count; TODO: maybe send also quorum size
        return nodeIdArg + " " + host + " " + port + " " + GetTransactionManagerUrlsArgument(transactionManagersUrls) + " " + GetLeaseManagerUrlsArgument(leaseManagersUrls) + " " + timeSlots + " " + starts + " " + lasts + " " + BuildProcessStateArgument(processIndex, processesState);
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
        return processStateArg;
    }
}