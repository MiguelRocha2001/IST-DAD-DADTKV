﻿using Google.Protobuf.Collections;
using Grpc.Net.Client;
using GrpcDADTKVClient;

// const string clientScriptFilename = "./DADTKV_client_script_sample";
const string clientScriptFilename = "C:/Users/migas/Repos/dad-project/Client/DADTKV_client_script_sample";

// The port number must match the port of the gRPC server.
using var channel = GrpcChannel.ForAddress("http://localhost:5000"); // grpc channel
var client = new DADTKV.DADTKVClient(channel); // grpc client

// infinite loop, executing the commands in the client script file
while (true) 
{
    // for some reason, running in debug mode, the client script file is not found (he assumes a different path, not sure why)
    IEnumerator<string> lines = File.ReadLines(clientScriptFilename).GetEnumerator(); 
    
    // executes a script operation (line)
    while (lines.MoveNext())
    {
        string line = lines.Current;
        if (!line.StartsWith('#')) // not a comment
        {
            if (line.StartsWith('W')) // Wait cmd
            {
                int waitTime = int.Parse(line.Split(' ')[1]);
                Console.WriteLine("Waiting " + waitTime + "ms");
                Task.Delay(waitTime).Wait();
                Console.WriteLine("Waited " + waitTime + "ms");
            }
            else if(line.StartsWith('T'))
            {
                string[] transaction = line.Split(' ').Skip(1).ToArray();
                
                string[] readSet = transaction[0].Split(',').ToArray();
                string[] writeSet = transaction[1].Split('>').ToArray();
                writeSet = writeSet.Take(writeSet.Length - 1).ToArray(); // removes last element (invalid)

                TxSubmitRequest request = new TxSubmitRequest(); // GRPC request
                request.Client = "client1";
                request.Reads.Add(readSet); // inserts read operations in the request

                string CleanInput(string input) => input.Replace("<", "").Replace(">", "").Replace("\"", "").Replace("(", "").Replace(")", "");

                foreach (string writeOper in writeSet) // inserts write operations in the request
                {
                    string writeOperAux = writeOper;
                    if (writeOperAux.StartsWith(',')) writeOperAux = writeOperAux.Substring(1); // removes first comma
                    
                    DadInt dadInt = new DadInt();
                    dadInt.Key = CleanInput(writeOperAux.Split(',')[0]);
                    dadInt.Value = long.Parse(CleanInput(writeOperAux.Split(',')[1]));
                    request.Writes.Add(dadInt);
                }

                Console.WriteLine("Sending transaction...");
                TxSubmitReply reply = await Task.FromResult(client.TxSubmit(request)); // sends the request to the server (transaction manager)
                Console.WriteLine("Transaction result:");
                foreach (DadInt dadInt in reply.Result) // prints the result of the transaction
                {
                    Console.WriteLine(dadInt.Key + " " + dadInt.Value);
                }
                Console.WriteLine("\n");
            }
            else
            {
                throw new Exception("Invalid command");
            }
        }
    }
}