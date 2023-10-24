using System.Diagnostics;
using utils;

const string systemConfigFilePath = "../configuration_sample1";
// const string systemConfigFilePath = "C:/Users/migas/Repos/dad-project/configuration_sample";
const string CLIENT_PROCESS_FILE_PATH = "../Client/bin/Debug/net6.0/Client";
const string TRANSACTION_MANAGER_PROCESS_FILE_PATH = "../TransactionManager/bin/Debug/net6.0/TransactionManager";
const string LEASE_MANAGER_PROCESS_FILE_PATH = "../LeaseManager/bin/Debug/net6.0/LeaseManager";

const bool START_PROCESSES = true;

List<string[]> processesConfig = new List<string[]>();
// for some reason, running in debug mode, the client script file is not found (he assumes a different path, not sure why)
IEnumerator<string> lines = File.ReadLines(systemConfigFilePath).GetEnumerator(); 

List<string> transactionManagersIds = new List<string>();
List<string> transactionManagersUrls = new List<string>();
List<string> leaseManagersUrls = new List<string>();

// TODO: later check if they are unseiged
int timeSlots = 0;
string starts = "";
int lasts = 0;

Dictionary<int, List<ProcessState>> processesState = new Dictionary<int, List<ProcessState>>();
Dictionary<int, HashSet<Tuple<string, string>>> processesSuspectedNodes = new Dictionary<int, HashSet<Tuple<string, string>>>();

// parse system script
while (lines.MoveNext())
{
    string line = lines.Current;
    if (line.StartsWith('P'))
    {
        string[] split = line.Split(' ');
        processesConfig.Add(split);
    }
    else
    {
        string[] split = line.Split(' ');
        if (split[0] == "S")
        {
            timeSlots = int.Parse(split[1]);
        }
        else if (split[0] == "T")
        {
            starts = split[1];
        }
        else if (split[0] == "D")
        {
            lasts = int.Parse(split[1]);
        }
        else if (split[0] == "F")
        {
            int timeSlot = int.Parse(split[1]);
            //Console.WriteLine($"timeSlot: {timeSlot}");
            processesState[timeSlot-1] = new List<ProcessState>();
            processesSuspectedNodes[timeSlot - 1] = new HashSet<Tuple<string, string>>();
            foreach (string state in split[2..])
            {
                if (state == "N" || state == "C") // N = normal, C = crashed
                    processesState[timeSlot-1].Add(state == "N" ? ProcessState.NORMAL : ProcessState.CRASHED);
                else
                {
                    string nodesClean = state.Trim('(').Trim(')');
                    string[] nodesCleanSplit = nodesClean.Split(',');
                    processesSuspectedNodes[timeSlot - 1].Add(new Tuple<string, string>(nodesCleanSplit[0], nodesCleanSplit[1]));
                }
            }
        }
    }
}

// parses server urls
foreach (string[] processConfig in processesConfig)
{
    string processType = processConfig[2];
    if (processType.StartsWith("T"))
    {
        transactionManagersIds.Add(processConfig[1]);
        transactionManagersUrls.Add(processConfig[3]);
    }
    else if (processType.StartsWith("L"))
    {
        leaseManagersUrls.Add(processConfig[3]);
    }
}

// launches processes
HashSet<Process> processes = new HashSet<Process>();
try
{
    // var p_info = new ProcessStartInfo
    // {
    //     UseShellExecute = true,
    //     CreateNoWindow = false,
    //     WindowStyle = ProcessWindowStyle.Normal,
    // };
    foreach (string[] processConfig in processesConfig)
    {
        int processIndex = processesConfig.IndexOf(processConfig);
        char type = processConfig[0][0];
        if (type == 'P') // process
        {
            string processType = processConfig[2];
            string nodeId = processConfig[1];

            string host = processConfig[3].Split(':')[1].Remove(0, 2);
            string port = processConfig[3].Split(':')[2];
            
            Process newProcess = new Process();

            if (processType == "T") // Transaction Manager
            {
                newProcess.StartInfo.FileName = TRANSACTION_MANAGER_PROCESS_FILE_PATH;
                newProcess.StartInfo.Arguments = Utils.BuildManagerArguments(
                    Utils.ProcessType.TRANSACTION_MANAGER,
                    nodeId, 
                    host, 
                    port,
                    processIndex, 
                    transactionManagersIds,
                    transactionManagersUrls, 
                    leaseManagersUrls, 
                    timeSlots, 
                    starts, 
                    lasts, 
                    processesState, 
                    processesSuspectedNodes
                );
            }
            else if (processType == "L") // Lease Manager
            {
                newProcess.StartInfo.FileName = LEASE_MANAGER_PROCESS_FILE_PATH;
                newProcess.StartInfo.Arguments = Utils.BuildManagerArguments(
                    Utils.ProcessType.LEASE_MANAGER,
                    nodeId, 
                    host, 
                    port,
                    processIndex, 
                    transactionManagersIds,
                    transactionManagersUrls, 
                    leaseManagersUrls, 
                    timeSlots, 
                    starts, 
                    lasts, 
                    processesState, 
                    processesSuspectedNodes
                );
            }
            else // Client
            {
                string clientScript = processConfig[3];
                newProcess.StartInfo.FileName = CLIENT_PROCESS_FILE_PATH;
                newProcess.StartInfo.Arguments = clientScript;
            }

            //newProcess.StartInfo.CreateNoWindow = false;
            newProcess.StartInfo.UseShellExecute = true;
            //newProcess.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            //Console.WriteLine($"Giro: {newProcess.StartInfo.CreateNoWindow}");
            //Console.WriteLine($"Giro: {newProcess.StartInfo.UseShellExecute}");
            //Console.WriteLine($"Giro: {newProcess.StartInfo.WindowStyle}");

            if (START_PROCESSES)
            {
                bool result = newProcess.Start();
                Console.WriteLine("Process start result: " + result);
                processes.Add(newProcess);
            }
        }
    }
}
catch (Exception e)
{
    Console.WriteLine(e.StackTrace);
    if (START_PROCESSES)
        Utils.KillProcesses(processes);
}

Console.ReadLine();

if (START_PROCESSES)
    Utils.KillProcesses(processes);