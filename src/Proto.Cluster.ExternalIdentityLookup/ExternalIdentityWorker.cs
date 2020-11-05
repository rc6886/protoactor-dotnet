using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto.Cluster.Utils;

namespace Proto.Cluster
{
    public class ExternalIdentityWorker : IActor
    {
        private static readonly ConcurrentSet<string> StaleMembers = new ConcurrentSet<string>();

        private readonly Cluster _cluster;
        private readonly ILogger _logger = Log.CreateLogger<ExternalIdentityWorker>();
        private readonly ExternalIdentityLookup _lookup;
        private readonly MemberList _memberList;
        private readonly IExternalIdentityStorage _storage;


        public ExternalIdentityWorker(ExternalIdentityLookup lookup)
        {
            _cluster = lookup.Cluster;
            _memberList = lookup.MemberList;
            _lookup = lookup;
            _storage = lookup.Storage;
        }

        public async Task ReceiveAsync(IContext context)
        {
            try
            {
                if (context.Message is GetPid msg)
                {
                    if (context.Sender == null)
                    {
                        _logger.LogCritical("No sender in GetPid request");
                        return;
                    }

                    if (_cluster.PidCache.TryGet(msg.ClusterIdentity, out var existing))
                    {
                        context.Respond(new PidResult
                            {
                                Pid = existing
                            }
                        );
                        return;
                    }

                    var pid = await GetWithGlobalLock(context.Sender!, msg.ClusterIdentity, CancellationToken.None);
                    context.Respond(new PidResult
                        {
                            Pid = pid
                        }
                    );
                }
            }
            catch (Exception x)
            {
                _logger.LogError(x, "Mongo Identity worker crashed {Id}", context.Self!.ToShortString());
                throw;
            }
        }

        private async Task<PID> GetWithGlobalLock(PID sender, ClusterIdentity clusterIdentity, CancellationToken ct)
        {
            var activation = await _storage.TryGetExistingActivationAsync(clusterIdentity, ct);
            //we got an existing activation, use this
            if (activation != null)
            {
                var existingPid = await ValidateAndMapToPid(clusterIdentity, activation);
                if (existingPid != null)
                {
                    return existingPid;
                }
            }

            //are there any members that can spawn this kind?
            //if not, just bail out
            var activator = _memberList.GetActivator(clusterIdentity.Kind, sender.Address);
            if (activator == null) return null;

            //try to acquire global lock
            var spawnLock = await _storage.TryAcquireLockAsync(clusterIdentity, ct);


            //we didn't get the lock, wait for activation to complete
            if (spawnLock == null)
                return await ValidateAndMapToPid(
                    clusterIdentity,
                    await _storage.WaitForActivationAsync(clusterIdentity, ct)
                );

            //we have the lock, spawn and return
            var pid = await SpawnActivationAsync(activator, spawnLock, ct);

            return pid;
        }

        private async Task<PID> SpawnActivationAsync(Member activator, SpawnLock spawnLock, CancellationToken ct)
        {
            //we own the lock
            _logger.LogDebug("Storing placement lookup for {Identity} {Kind}", spawnLock.ClusterIdentity.Identity,
                spawnLock.ClusterIdentity.Kind
            );

            var remotePid = _lookup.RemotePlacementActor(activator.Address);
            var req = new ActivationRequest
            {
                ClusterIdentity = spawnLock.ClusterIdentity
            };

            try
            {
                var resp = ct == CancellationToken.None
                    ? await _cluster.System.Root.RequestAsync<ActivationResponse>(remotePid, req,
                        _cluster.Config!.TimeoutTimespan
                    )
                    : await _cluster.System.Root.RequestAsync<ActivationResponse>(remotePid, req, ct);

                if (resp.Pid != null)
                {
                    try
                    {
                        await _storage.StoreActivation(activator, spawnLock, resp.Pid, ct);
                    }
                    catch (StorageFailure e)
                    {
                        //meaning, we spawned an actor but its placement is not stored anywhere
                        _logger.LogCritical(e, "No entry was updated {@SpawnLock}", spawnLock);
                    }

                    _cluster.PidCache.TryAdd(spawnLock.ClusterIdentity, resp.Pid!);
                    return resp.Pid;
                }
            }
            //TODO: decide if we throw or return null
            catch (TimeoutException)
            {
                _logger.LogDebug("Remote PID request timeout {@Request}", req);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error occured requesting remote PID {@Request}", req);
            }

            //Clean up our mess..
            await _storage.RemoveLock(spawnLock, ct);
            return null;
        }


        private async Task<PID> ValidateAndMapToPid(ClusterIdentity clusterIdentity, StoredActivation activation)
        {
            var memberExists = activation.MemberId == null || _memberList.ContainsMemberId(activation.MemberId);
            if (!memberExists)
            {
                if (StaleMembers.TryAdd(activation.MemberId))
                {
                    _logger.LogWarning(
                        "Found placement lookup for {ClusterIdentity}, but Member {MemberId} is not part of cluster, dropping stale entries",
                        clusterIdentity.ToShortString(), activation.MemberId
                    );
                }


                //let all requests try to remove, but only log on the first occurrence
                await _storage.RemoveMemberIdAsync(activation.MemberId, CancellationToken.None);
                return null;
            }

            return activation.Pid;
        }
    }
}