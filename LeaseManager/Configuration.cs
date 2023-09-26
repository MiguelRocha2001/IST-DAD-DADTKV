class PaxosNodes {
    public string node;
    public HashSet<string> nodes;
    public PaxosNodes(string node)
    {
        this.node = node;
        nodes = new HashSet<string> // for testing purposes
        {
            "http://localhost:6001",
            "http://localhost:6002",
            //"http://localhost:6003"
        };
    }
}