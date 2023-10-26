using System.ComponentModel;
using Google.Protobuf.Collections;
using Grpc.Net.Client;
using GrpcDADTKVClient;
public enum CommandType { Transaction, Wait, Status }
class Program
{
    static void Main(string[] args)
    {
        Thread.Sleep(1); // Used for testing only

        string clientScriptFilename = "DADTKV_client_script_sample";

        string transactionManagerServer;
        if (args[0] == "0")
        {
            transactionManagerServer = "http://localhost:5001";
        }
        else
        {
            transactionManagerServer = "http://localhost:5002";
        }


        // The port number must match the port of the gRPC server.
        using var channel = GrpcChannel.ForAddress(transactionManagerServer); // grpc channel
        var client = new DADTKV.DADTKVClient(channel); // grpc client

        List<Tuple<CommandType, string[]>> CommandList = new List<Tuple<CommandType, string[]>>();

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
                    CommandList.Add(Tuple.Create(CommandType.Wait, new[] { line.Split(' ')[1] }));
                }
                else if (line.StartsWith('T'))
                {
                    string[] transaction = line.Split(' ').Skip(1).ToArray();
                    string CleanInput(string input) => input.Replace("<", "").Replace(">", "").Replace("\"", "").Replace("(", "").Replace(")", "");

                    // tuple of the form (Transaction, ["a-key-name,another-key-name","name1,10,name2,20"])
                    CommandList.Add(Tuple.Create(CommandType.Transaction, new[] { CleanInput(transaction[0]), CleanInput(transaction[1]) }));
                }
                else
                {
                    throw new Exception("Invalid command");
                }
            }
        }

        while (true)
        {
            foreach (Tuple<CommandType, string[]> CommandPair in CommandList)
            {
                switch (CommandPair.Item1)
                {
                    case CommandType.Transaction:

                        string[] readSet = CommandPair.Item2[0].Split(',');
                        string[] writeSet = CommandPair.Item2[1].Split(',');

                        TxSubmitRequest request = new TxSubmitRequest(); // GRPC request
                        request.Client = "client1";
                        request.Reads.Add(readSet); // inserts read operations in the request

                        for (int i = 0; i < writeSet.Length-1; i+=2)
                        {
                            DadInt dadInt = new DadInt();
                            dadInt.Key = writeSet[i];
                            dadInt.Value = int.Parse(writeSet[i+1]);
                            request.Writes.Add(dadInt);
                        }

                        // Console.WriteLine("Sending transaction...");
                        // TxSubmitReply reply = await Task.FromResult(client.TxSubmit(request)); // sends the request to the server (transaction manager)
                        // Console.WriteLine("Transaction result:");
                        // foreach (DadInt dadInt in reply.Result) // prints the result of the transaction
                        // {
                        //     Console.WriteLine(dadInt.Key + " " + dadInt.Value);
                        // }
                        Console.WriteLine("\n");
                        break;
                    case CommandType.Wait:
                        int waitTime = int.Parse(CommandPair.Item2[0]);
                        Console.WriteLine("Waiting " + waitTime + "ms");
                        Task.Delay(waitTime).Wait();
                        Console.WriteLine("Waited " + waitTime + "ms");
                        break;
                    case CommandType.Status:
                        break;
                }
            }
        }
    }
}
