using System.Threading;
using Proto.Router;

namespace Proto.Cluster
{
    public class GetPid : IHashable
    {
        public ClusterIdentity ClusterIdentity { get; set; }
        public CancellationToken CancellationToken { get; set; }
        public string HashBy() => ClusterIdentity.ToShortString();
    }

    public class PidResult
    {
        public PID Pid { get; set; }
    }
}