﻿using System.Diagnostics;

void KillProcesses(HashSet<Process> processes)
{
    Console.WriteLine("Killing all processes!");
    foreach (Process process in processes)
    {
        process.Kill();
    }
    Console.WriteLine("Processes killed!");
}

const string systemConfigFilePath = "../configuration_sample";
// const string systemConfigFilePath = "C:/Users/migas/Repos/dad-project/configuration_sample";
const string CLIENT_PROCESS_FILE_PATH = "TODO";
const string TRANSACTION_MANAGER_PROCESS_FILE_PATH = "../TransactionManager/bin/Debug/net6.0/TransactionManager";
const string LEASE_MANAGER_PROCESS_FILE_PATH = "../LeaseManager/bin/Debug/net6.0/LeaseManager";

List<string[]> processesConfig = new List<string[]>();
// for some reason, running in debug mode, the client script file is not found (he assumes a different path, not sure why)
IEnumerator<string> lines = File.ReadLines(systemConfigFilePath).GetEnumerator(); 

// parse system script
while (lines.MoveNext())
{
    string line = lines.Current;
    if (line.StartsWith('P'))
    {
        string[] split = line.Split(' ');
        //List<string> splitAux = split.ToList();
        //splitAux.RemoveAt(0);
        processesConfig.Add(split);
    }
}

// parses server urls
List<String> transactionManagersUrls = new List<string>();
List<String> leaseManagersUrls = new List<string>();
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
        char type = processConfig[0][0];
        if (type == 'P') // process
        {
            string processType = processConfig[2];
            string nodeId = processConfig[1];

            if (processType == "T" || processType == "L")
            {
                string href = processConfig[3];
                
                Process newProcess = new Process();
                processes.Add(newProcess);

                if (processType == "T") // transaction manager
                {
                    newProcess.StartInfo.FileName = TRANSACTION_MANAGER_PROCESS_FILE_PATH;
                    newProcess.StartInfo.Arguments = nodeId.Last().ToString();
                }
                else // lease manager
                {
                    newProcess.StartInfo.FileName = LEASE_MANAGER_PROCESS_FILE_PATH;
                    newProcess.StartInfo.Arguments = nodeId.Last().ToString();
                }
                                                 
                newProcess.Start();
                Console.WriteLine("Process started.");
            }
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