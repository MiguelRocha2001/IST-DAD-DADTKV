using System.Collections.Immutable;
using Microsoft.Net.Http.Headers;

class Paxos {
    List<RequestLeaseRequest> requests = new List<RequestLeaseRequest>();
    HashSet<string> nodes = new HashSet<string>(); // represents the nodes in the cluster
    string node; // represents the name of this node
    Boolean isLeader = false;

    public Paxos(string node, HashSet<string> nodes)
    {
        this.node = node;
        this.nodes = nodes;
        DefineLeader();
        ExecutePaxosEpoch();
    }

    public void DefineLeader()
    {
        IEnumerable<int> nodesCastedToInt = nodes.Cast<int>(); // cast the nodes to int
        int min = nodesCastedToInt.Min(); // get the min value
        isLeader = min == int.Parse(node);
    }

    public void ExecutePaxosEpoch()
    {
        if (isLeader)
        {
            bool aproved = BroadcastPrepareRequest();
            if (aproved)
            {
                BroadcasteAcceptRequest();
            }
        }
    }
