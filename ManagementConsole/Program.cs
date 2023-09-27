/*using System.Diagnostics;

const string systemConfigFilePath = "../configuration_sample";
// const string systemConfigFilePath = "C:/Users/migas/Repos/dad-project/configuration_sample";
const string CLIENT_PROCESS_FILE_PATH = "TODO";
const string TRANSACTION_MANAGER_PROCESS_FILE_PATH = "TODO";
const string LEASE_MANAGER_PROCESS_FILE_PATH = "TODO";

// infinite loop, executing the commands in the client script file
while (true) 
{
    // for some reason, running in debug mode, the client script file is not found (he assumes a different path, not sure why)
    IEnumerator<string> lines = File.ReadLines(systemConfigFilePath).GetEnumerator(); 
    
    // executes a script operation (line)
    while (lines.MoveNext())
    {
        string line = lines.Current;
        string[] split = line.Split(' ');
        if (line.StartsWith('P'))
        {
            string nodeName = split[1];
            string processType = split[2];
            if (processType == "T" || processType == "L" || processType == "C")
            {
                int href = int.Parse(split[3]);
                
                Process newProcess = new Process();
                if (processType == "T") // transaction manager
                {
                    newProcess.StartInfo.FileName = TRANSACTION_MANAGER_PROCESS_FILE_PATH;
                }
                else if (processType == "L") // lease manager
                {
                    newProcess.StartInfo.FileName = LEASE_MANAGER_PROCESS_FILE_PATH;
                }
                else if (processType == "C") // client
                {
                    newProcess.StartInfo.FileName = CLIENT_PROCESS_FILE_PATH;
                }
                
                newProcess.StartInfo.Arguments = nodeName;
                newProcess.Start();
                Console.WriteLine("Process started.");
            }
        }
        else if (line.StartsWith('S'))
        {
            string timeSlots = split[1];
        }
        else if (line.StartsWith('T'))
        {
            string physicalWallTime = split[1];
        }
    }
}*/

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


