using System.Diagnostics;

enum ProcessState { NORMAL, CRASHED }

const string systemConfigFilePath = "../configuration_sample";
// const string systemConfigFilePath = "C:/Users/migas/Repos/dad-project/configuration_sample";
const string CLIENT_PROCESS_FILE_PATH = "../Client/bin/Debug/net6.0/Client";
const string TRANSACTION_MANAGER_PROCESS_FILE_PATH = "../TransactionManager/bin/Debug/net6.0/TransactionManager";
const string LEASE_MANAGER_PROCESS_FILE_PATH = "../LeaseManager/bin/Debug/net6.0/LeaseManager";

List<string[]> processesConfig = new List<string[]>();
// for some reason, running in debug mode, the client script file is not found (he assumes a different path, not sure why)
IEnumerator<string> lines = File.ReadLines(systemConfigFilePath).GetEnumerator(); 

List<String> transactionManagersUrls = new List<string>();
List<String> leaseManagersUrls = new List<string>();

// TODO: later check if they are unseiged
int timeSlots = 0;
string starts = "";
int lasts = 0;

List<List<ProcessState>> processesState = new List<List<ProcessState>>();

void KillProcesses(HashSet<Process> processes)
{
    Console.WriteLine("Killing all processes!");
    foreach (Process process in processes)
    {
        process.Kill();
    }
    Console.WriteLine("Processes killed!");
}

string GetTransactionManagerUrlsArgument()
{
    string transactionManagersArg = "[";
    foreach (string transactionManagerUrl in transactionManagersUrls)
    {
        transactionManagersArg += transactionManagerUrl + ",";
    }
    transactionManagersArg = transactionManagersArg.Trim(','); // removes last comma
    transactionManagersArg += "]";

    return transactionManagersArg;
}

string BuildLeaseManagerArguments(string nodeId)
{
    string nodeIdArg = nodeId.Last().ToString();
    string leaseManagersArg = "[";
    foreach (string leaseManagerUrl in leaseManagersUrls)
    {
        leaseManagersArg += leaseManagerUrl + ",";
    }
    leaseManagersArg = leaseManagersArg.Trim(','); // removes last comma
    leaseManagersArg += "]";

    return nodeIdArg + " " + leaseManagersArg + " " + GetTransactionManagerUrlsArgument() + " " + timeSlots + " " + starts + " " + lasts;
}

string BuildTransactionManagerArguments(string nodeId, int processIndex)
{
    string nodeIdArg = nodeId.Last().ToString();
    int quorumSize = leaseManagersUrls.Count;
    return nodeIdArg + " " + GetTransactionManagerUrlsArgument() + " " + quorumSize + " " + timeSlots + " " + starts + " " + lasts + " " + BuildProcessStateArgument(processIndex);
}

string BuildProcessStateArgument(int processIndex)
{
    string processStateArg = "[";
    List<ProcessState> processStates = processesState[processIndex]; // fetches process states for this process
    foreach (ProcessState processState in processStates)
    {
        processStateArg += processState == ProcessState.NORMAL ? "N" : "C";
        processStateArg += ",";
    }
    processStateArg = processStateArg.Trim(','); // removes last comma
    processStateArg += "]";
    return processStateArg;
}

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
            foreach (string node in split[2..])
            {
                if (node != "N" && node != "C") // N = normal, C = crashed
                    break;
                processesState[timeSlot].Add(node == "N" ? ProcessState.NORMAL : ProcessState.CRASHED);
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
    foreach (string[] processConfig in processesConfig)
    {
        int processIndex = processesConfig.IndexOf(processConfig);
        char type = processConfig[0][0];
        if (type == 'P') // process
        {
            string processType = processConfig[2];
            string nodeId = processConfig[1];

            string thirdArgument = processConfig[3];
            
            Process newProcess = new Process();
            processes.Add(newProcess);

            if (processType == "T") // Transaction Manager
            {
                newProcess.StartInfo.FileName = TRANSACTION_MANAGER_PROCESS_FILE_PATH;
                newProcess.StartInfo.Arguments = BuildTransactionManagerArguments(nodeId, processIndex);
            }
            else if (processType == "L") // Lease Manager
            {
                newProcess.StartInfo.FileName = LEASE_MANAGER_PROCESS_FILE_PATH;
                newProcess.StartInfo.Arguments = BuildLeaseManagerArguments(nodeId);
            }
            else // Client
            {
                string clientScript = processConfig[3];
                newProcess.StartInfo.FileName = CLIENT_PROCESS_FILE_PATH;
                newProcess.StartInfo.Arguments = clientScript;
            }
                                                
            newProcess.Start();
            Console.WriteLine("Process started.");
            
        }
    }
}
catch
{
    KillProcesses(processes);
    return;
}

Console.ReadLine();
KillProcesses(processes);

/*
int count = 0;
List<Task> tasks = new();
for (int i = 0; i < 30; i++){
    var a = i;
    tasks.Add(Task.Run(async () =>
    {
        await Task.Delay(1000);
        Interlocked.Increment(ref count);
        Console.WriteLine($"Count inside task {a} {count}");
    }));
}

Task.WaitAll(tasks.ToArray());

Console.WriteLine($"Count Outside task {count}");
*/