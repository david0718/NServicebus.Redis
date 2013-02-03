using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Timeout.Core;
using ServiceStack.Redis;

namespace NServiceBus.Redis
{

	public class SatelliteTransportBuilderInitialization : IWantToRunBeforeConfigurationIsFinalized
	{

		#region IWantToRunBeforeConfigurationIsFinalized Members

		public void Run()
		{
			//Need to override this component because the default one has a hard dependency on MSMQ
			Configure.Instance.Configurer.ConfigureComponent<SatelliteTransportBuilder>(DependencyLifecycle.SingleInstance);         
		}

		#endregion
	}

	public static class ConfigurationExtensions
	{
		private static void ConfigureRedisClientManager(Configure config, params string[] readWriteHosts)
		{
			if (!config.Configurer.HasComponent<ServiceStack.Redis.PooledRedisClientManager>())
			{
				config.Configurer.ConfigureComponent<ServiceStack.Redis.PooledRedisClientManager>(() =>
				{
					return new ServiceStack.Redis.PooledRedisClientManager(GetHosts(readWriteHosts));
				},
				DependencyLifecycle.SingleInstance);
			}
		}

		private static void ConfigureSerializer(Configure config)
		{
			if (!config.Configurer.HasComponent<ISerializer>())
				config.Configurer.ConfigureComponent<ISerializer>(() => new JsonSerializer(), DependencyLifecycle.SingleInstance);
		}

		private static string[] GetHosts(string [] hosts)
		{
			if (hosts == null || hosts.Length == 0)
			{
				//Get from config
				string hostsString = System.Configuration.ConfigurationManager.AppSettings["NServiceBus.Redis/Hosts"];

				if (hostsString != null)
				{
					return hostsString.Split(new[] { ',', ';' }).Where(o => o.Length > 0).ToArray();
				}
				else
				{
					throw new ConfigurationErrorsException("No hosts provided and no config found. Please make sure \"NServiceBus.Redis/Hosts\" is added to <appSettings>");
				}
			}
			else
			{
				return hosts;
			}
		}

		public static Configure RedisTransport(this Configure config, bool sharedQueues, params string[] readWriteHosts)
		{
			ConfigureRedisClientManager(config, readWriteHosts);
			config.Configurer.ConfigureComponent<RedisQueue>(() => 
			{
				return new RedisQueue(
					new JsonSerializer(), 
					new PooledRedisClientManager(GetHosts(readWriteHosts)), 
					new QueueKeyNameProvider(sharedQueues), 
					60
				);
			},
			DependencyLifecycle.InstancePerCall);
			
			return config;
		}

		public static Configure RedisSubscriptionStorage(this Configure config, params string[] readWriteHosts)
		{
			config.Configurer.ConfigureComponent<RedisSubscriptionStorage>(() => 
			{
				return new RedisSubscriptionStorage(new JsonSerializer(), new PooledRedisClientManager(GetHosts(readWriteHosts)));
			},
			DependencyLifecycle.SingleInstance);

			return config;
		}

		public static Configure RedisTimeoutStorage(this Configure config, params string[] readWriteHosts)
		{
			//config.Configurer.ConfigureComponent<DefaultTimeoutManager>(DependencyLifecycle.SingleInstance);
			config.Configurer.ConfigureComponent<RedisTimeoutPersistence>(() => 
			{
				return new RedisTimeoutPersistence(new JsonSerializer(), new PooledRedisClientManager(GetHosts(readWriteHosts)));
			},
			DependencyLifecycle.SingleInstance).ConfigureProperty(p => p.EndpointName, NServiceBus.Configure.EndpointName);

			return config;
		}

		public static Configure RedisSagaStorage(this Configure config, params string[] readWriteHosts)
		{
			config.Configurer.ConfigureComponent<RedisSagaPersister>(() => 
			{
				return new RedisSagaPersister(new JsonSerializer(), new PooledRedisClientManager(GetHosts(readWriteHosts)));
			},DependencyLifecycle.SingleInstance);

			return config;
		}

		public static Configure RedisForEverything(this Configure config, bool sharedQueues, params string[] readWriteHosts)
		{
			RedisTransport(config, sharedQueues, readWriteHosts);
			RedisTimeoutStorage(config, readWriteHosts);
			RedisSubscriptionStorage(config, readWriteHosts);
			RedisSagaStorage(config, readWriteHosts);
			
			return config;
		}
	}
}
