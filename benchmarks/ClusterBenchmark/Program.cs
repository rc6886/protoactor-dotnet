﻿using System;
using System.Threading;
using System.Threading.Tasks;
using ClusterExperiment1.Messages;
using k8s;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Proto;
using Proto.Cluster;
using Proto.Cluster.Consul;
using Proto.Cluster.IdentityLookup;
using Proto.Cluster.Kubernetes;
using Proto.Cluster.MongoIdentityLookup;
using Proto.Remote;

namespace ClusterExperiment1
{
    public static class Program
    {
        private static Task RunFollower()
        {
            SetupLogger();

            SpawnMember();

            Thread.Sleep(Timeout.Infinite);
            return Task.CompletedTask;
        }

        private static async Task RunLeader()
        {
            var logger = SetupLogger();

            var cluster = await SpawnClient();

            await Task.Delay(5000);

            _ = Task.Run(async () =>
                {
                    var rnd = new Random();
                    while (true)
                    {
                        var id = "myactor" + rnd.Next(0, 1000);
                        try
                        {
                            var res = await cluster.RequestAsync<HelloResponse>(id, "hello", new HelloRequest(),
                                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token
                            );

                            if (res == null)
                            {
                                logger.LogError("Null response");
                            }
                            else
                            {
                                Console.Write(".");
                            }
                        }
                        catch (Exception x)
                        {
                            logger.LogError(x, "Request timeout for {Id}", id);
                        }
                    }
                }
            );

            Console.ReadLine();

            //   Thread.Sleep(Timeout.Infinite);
        }

        private static ILogger SetupLogger()
        {
            Log.SetLoggerFactory(LoggerFactory.Create(l => l.AddConsole(o =>
                        {
                            o.IncludeScopes = false;
                            o.UseUtcTimestamp = false;
                            o.TimestampFormat = "hh:mm:ss:fff - ";
                        }
                    ).SetMinimumLevel(LogLevel.Information)
                )
            );
            var logger = Log.CreateLogger(nameof(Program));
            return logger;
        }


        public static async Task Main(string[] args)
        {
            if (args.Length == 0)
            {
                await RunLeader();
            }
            else
            {
                await RunFollower();
            }
        }

        private static async Task<Cluster> SpawnClient()
        {
            var system = new ActorSystem();
            var clusterProvider = ClusterProvider();
            var identity = GetIdentityLookup();
            var config = GetClusterConfig(clusterProvider, identity);
            var cluster = new Cluster(system, config);
            await cluster.StartClientAsync();
            return cluster;
        }

        private static Cluster SpawnMember()
        {
            var system = new ActorSystem();
            var clusterProvider = ClusterProvider();
            var identity = GetIdentityLookup();
            var helloProps = Props.FromProducer(() => new HelloActor());
            var config = GetClusterConfig(clusterProvider, identity)
                .WithClusterKind("hello", helloProps);

            var cluster = new Cluster(system, config);

            cluster.StartMemberAsync();
            return cluster;
        }

        private static ClusterConfig GetClusterConfig(IClusterProvider clusterProvider, IIdentityLookup identityLookup)
        {
            var portStr = Environment.GetEnvironmentVariable("PROTOPORT") ?? $"{RemoteConfig.AnyFreePort}";
            var port = int.Parse(portStr);
            var host = Environment.GetEnvironmentVariable("PROTOHOST") ?? RemoteConfig.Localhost;
            var advertisedHost = Environment.GetEnvironmentVariable("PROTOHOSTPUBLIC");
            return ClusterConfig
                .Setup("mycluster", clusterProvider, identityLookup,
                    RemoteConfig
                        .BindTo(host, port)
                        .WithAdvertisedHost(advertisedHost)
                        .WithProtoMessages(MessagesReflection.Descriptor)
                );
        }

        private static IClusterProvider ClusterProvider()
        {
            try
            {
                Console.WriteLine("Running with InClusterConfig");

                var kubernetesConfig =
                    KubernetesClientConfiguration
                        .InClusterConfig(); //   KubernetesClientConfiguration.BuildConfigFromConfigFile(cachedNamespace);
                var kubernetes = new Kubernetes(kubernetesConfig);
                return new KubernetesProvider(kubernetes);
            }
            catch
            {
                return new ConsulProvider(new ConsulProviderConfig());
            }
        }

        private static IIdentityLookup GetIdentityLookup()
        {
            var db = GetMongo();
            var identity =
                new ExternalIdentityLookup(new MongoIdentityStorage("mycluster",
                        db.GetCollection<PidLookupEntity>("pids")
                    )
                );
            return identity;
        }

        static IMongoDatabase GetMongo()
        {
            var connectionString =
                Environment.GetEnvironmentVariable("MONGO") ?? "mongodb://127.0.0.1:27017/ProtoMongo";
            var url = MongoUrl.Create(connectionString);
            var settings = MongoClientSettings.FromUrl(url);
            settings.WriteConcern = WriteConcern.Acknowledged;
            settings.ReadConcern = ReadConcern.Majority;
            var client = new MongoClient(settings);
            var database = client.GetDatabase("ProtoMongo");
            return database;
        }
    }


    public class HelloActor : IActor
    {
        //   private readonly ILogger _log = Log.CreateLogger<HelloActor>();

        public Task ReceiveAsync(IContext ctx)
        {
            if (ctx.Message is Started)
            {
                //just to highlight when this happens
                Console.Write("#");
                //_log.LogInformation("I started " + ctx.Self);
            }

            if (ctx.Message is HelloRequest)
            {
                ctx.Respond(new HelloResponse());
            }

            if (ctx.Message is Stopped)
            {
                //just to highlight when this happens
                //    _log.LogWarning("I stopped" + ctx.Self);
            }

            return Task.CompletedTask;
        }
    }
}