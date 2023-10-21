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

        List<Tuple<CommandType, string>> CommandList = new List<Tuple<CommandType, string>>();

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
                    CommandList.Add(Tuple.Create(CommandType.Wait, line.Split(' ')[1]));
                }
                else if (line.StartsWith('T'))
                {
                    CommandList.Add(Tuple.Create(CommandType.Transaction, line));
                }
                else
                {
                    throw new Exception("Invalid command");
                }
            }
        }

        while (true)
        {
            foreach (Tuple<CommandType, string> CommandPair in CommandList)
            {
                switch (CommandPair.Item1)
                {
                    case CommandType.Transaction:
                        string[] transaction = CommandPair.Item2.Split(' ').Skip(1).ToArray();
                        string CleanPar(string input) => input.Replace("(", "").Replace(")", "").Replace("\"", "");
                        string[] readSet = CleanPar(transaction[0]).Split(',').ToArray();

                        foreach (string i in readSet)
                            Console.WriteLine(i);
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
                        int waitTime = int.Parse(CommandPair.Item2);
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
