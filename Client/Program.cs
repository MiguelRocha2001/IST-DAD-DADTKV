using Google.Protobuf.Collections;
using Grpc.Net.Client;
using GrpcDADTKVClient;

const string clientScriptFilename = "./DADTKV_client_script_sample";

// The port number must match the port of the gRPC server.
using var channel = GrpcChannel.ForAddress("http://localhost:5000"); // grpc channel
var client = new DADTKV.DADTKVClient(channel); // grpc client

IEnumerator<string> lines = File.ReadLines(clientScriptFilename).GetEnumerator();

while (true) 
{
    while (lines.MoveNext())
    {
        string line = lines.Current;
        if (!line.StartsWith('#')) // not a comment
        {
            Console.WriteLine(line);
            if (line.StartsWith('W')) // Wait cmd
            {
                int waitTime = int.Parse(line.Split(' ')[1]);
                Task.Delay(waitTime).Wait();
            }
            else if(line.StartsWith('T'))
            {
                string[] transaction = line.Split(' ').Skip(1).ToArray();
                
                string[] readSet = transaction[0].Split(',').ToArray();
                string[] writeSet = transaction[1].Split(',').ToArray();

                TxSubmitRequest request = new TxSubmitRequest();
                request.Client = "client1";
                request.Reads.Add(readSet);
                foreach (string writeOper in writeSet) // executes write operations
                {
                    DadInt dadInt = new DadInt();
                    dadInt.Key = writeOper.Split(',')[0].Replace("<", "").Replace(">", "").Replace("\"", "");
                    dadInt.Value = int.Parse(writeOper.Split(',')[1]);
                    request.Writes.Add(dadInt);
                }
                client.TxSubmit(request); // sends the request to the server (transaction manager)
            }
            else
            {
                throw new Exception("Invalid command");
            }
        }
    }
}