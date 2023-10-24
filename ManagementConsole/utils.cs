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

    static string ListStringToArgument(List<String> listOfStrings)
    {
        string strArg = "";
        foreach (string str in listOfStrings)
        {
            strArg += str + ",";
        }
        strArg = strArg.Trim(','); // removes last comma
        strArg += "";

        return strArg;
    }

    static string GetManagerUrlsArgument(List<string> leaseManagerUrls)
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

    static public string BuildManagerArguments(
        ProcessType processType,
        string nodeId, 
        string host,
        string port,
        int processIndex,
        List<String> transactionManagersIds,
        List<String> transactionManagersUrls,
        List<String> leaseManagersUrls,
        int timeSlots,
        string starts,
        int lasts,
        Dictionary<int, List<ProcessState>> processesState,
        Dictionary<int, HashSet<Tuple<string, string>>> processesSuspectedNodes
    )
    {
        // only necessary for TM nodes...
        string transactionManagerIds = 
            processType == ProcessType.TRANSACTION_MANAGER ? ListStringToArgument(transactionManagersIds) + " " : "";

        string t = nodeId + " " + host + " " + port + " " + 
        transactionManagerIds + 
        ListStringToArgument(transactionManagersUrls) + " " + 
        ListStringToArgument(leaseManagersUrls) + " " + 
        timeSlots + " " + starts + " " + lasts + " " + 
        BuildProcessStateArgument(processIndex, processesState) + " " +
        BuildProcessSuspectedStateArgument(processType, nodeId, processesSuspectedNodes);

        Console.WriteLine("Arguments: " + t);

        return t;
    }

    public enum ProcessType {TRANSACTION_MANAGER, LEASE_MANAGER};

    static string BuildProcessStateArgument(
        int processIndex,
        Dictionary<int, List<ProcessState>> processesState
    )
    {
        string processStateArg = "";
        foreach (KeyValuePair<int, List<ProcessState>> entry in processesState)
        {
            string state = entry.Key + (entry.Value[processIndex] == ProcessState.NORMAL ? "N" : "C");
            processStateArg += state + ",";
        }
        processStateArg = processStateArg.Trim(','); // removes last comma
        return processStateArg;
    }

    static string BuildProcessSuspectedStateArgument(
        ProcessType processType,
        string nodeId,
        Dictionary<int, HashSet<Tuple<string, string>>> processesSuspectedNodes
    )
    {
        string output = "";
        // each epoch
        foreach (KeyValuePair<int, HashSet<Tuple<string, string>>> entry in processesSuspectedNodes)
        {
            string strToAppend = "";
            bool found = false;
            // each suspect pair
            foreach (Tuple<string, string> tuple in entry.Value)
            {            
                if (tuple.Item1 == nodeId || tuple.Item2 == nodeId)
                {
                    found = true;

                    string idToInsert = tuple.Item1 == nodeId ? tuple.Item2 : tuple.Item1;
                    strToAppend += idToInsert + ',';
                }
            }
            strToAppend = strToAppend.Trim(',');

            // ads epoch
            if (found)
            {
                output += entry.Key + strToAppend; // epoch
                output += ';';
            }
        }
        output = output.Trim(';');

        if (processType == ProcessType.LEASE_MANAGER)
        {
            output += " ";

            // each epoch
            foreach (KeyValuePair<int, HashSet<Tuple<string, string>>> entry in processesSuspectedNodes)
            {
                string strToAppend = "";
                bool found = false;
                // each suspect pair
                foreach (Tuple<string, string> tuple in entry.Value)
                {            
                    if (tuple.Item1 == nodeId)
                    {
                        found = true;

                        string idToInsert = tuple.Item2;
                        strToAppend += idToInsert + ',';
                    }
                }
                strToAppend = strToAppend.Trim(',');

                // ads epoch
                if (found)
                {
                    output += entry.Key + strToAppend; // epoch
                    output += ';';
                }
            }
            output = output.Trim(';');
        }
        return output;
    }
}