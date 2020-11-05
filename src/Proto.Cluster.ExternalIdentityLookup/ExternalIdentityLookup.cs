using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto.Cluster.IdentityLookup;
using Proto.Router;

namespace Proto.Cluster
{
    public class ExternalIdentityLookup : IIdentityLookup
    {
        internal IExternalIdentityStorage Storage { get; }
        private const string PlacementActorName = "placement-activator";
        internal Cluster Cluster;
        private bool _isClient;
        private ILogger _logger;
        internal MemberList MemberList;
        private PID _placementActor;
        private ActorSystem _system;
        private PID _router;
        private string _memberId;

        public ExternalIdentityLookup(IExternalIdentityStorage storage)
        {
            Storage = storage;
        }

        public async Task<PID> GetAsync(ClusterIdentity clusterIdentity, CancellationToken ct)
        {
            var msg = new GetPid
            {
                ClusterIdentity = clusterIdentity,
                CancellationToken = ct
            };

            var res = await _system.Root.RequestAsync<PidResult>(_router, msg, ct);
            return res.Pid;
        }

        public Task SetupAsync(Cluster cluster, string[] kinds, bool isClient)
        {
            Cluster = cluster;
            _system = cluster.System;
            _memberId = cluster.Id.ToString();
            MemberList = cluster.MemberList;
            _logger = Log.CreateLogger("ExternalIdentityLookup-" + cluster.LoggerId);
            _isClient = isClient;

            var workerProps = Props.FromProducer(() => new ExternalIdentityWorker(this));
            //TODO: should pool size be configurable?

            var routerProps = _system.Root.NewConsistentHashPool(workerProps, 50);

            _router = _system.Root.Spawn(routerProps);

            //hook up events
            cluster.System.EventStream.Subscribe<ClusterTopology>(e =>
                {
                    //delete all members that have left from the lookup
                    foreach (var left in e.Left)
                        //YOLO. event stream is not async
                        _ = RemoveMemberAsync(left.Id);
                }
            );

            if (isClient) return Task.CompletedTask;
            var props = Props.FromProducer(() => new ExternalIdentityPlacementActor(Cluster,this));
            _placementActor = _system.Root.SpawnNamed(props, PlacementActorName);

            return Task.CompletedTask;
        }

        public async Task ShutdownAsync()
        {
            if (!_isClient) await Cluster.System.Root.PoisonAsync(_placementActor);

            await RemoveMemberAsync(Cluster.Id.ToString());
        }

        internal Task RemoveMemberAsync(string memberId)
        {
            return Storage.RemoveMemberIdAsync(memberId,CancellationToken.None);
        }

        internal PID RemotePlacementActor(string address)
        {
            return PID.FromAddress(address, PlacementActorName);
        }

        public Task RemovePidAsync(PID pid)
        {
            return Storage.RemoveActivation(pid, CancellationToken.None);
        }
    }
}