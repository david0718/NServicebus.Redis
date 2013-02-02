using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus;
using NServiceBus.Unicast.Subscriptions;
using ServiceStack.Redis;

namespace NServiceBus.Redis
{

	public class RedisSubscriptionStorage : ISubscriptionStorage
	{
		protected IRedisClientsManager _clientManager;
		protected ISerializer _serializer;

		public RedisSubscriptionStorage(ISerializer serializer, IRedisClientsManager clientManager)
		{
			_clientManager = clientManager;
			_serializer = serializer;
		}

		protected RedisClient GetClient()
		{
			return _clientManager.GetClient() as RedisClient;
		}

		#region ISubscriptionStorage Members

		public IEnumerable<Address> GetSubscriberAddressesForMessage(IEnumerable<MessageType> messageTypes)
		{
			var subscribers = new List<Address>();

			using (var client = GetClient())
			{
				foreach (var messageType in messageTypes)
				{
					var messageAddresses = client.Sets[GetSetName(messageType)].GetAll();
					if (messageAddresses != null && messageAddresses.Count > 0)
					{
						subscribers.AddRange(messageAddresses.Select(o => Address.Parse(o)));
					}
				}
			}

			return subscribers;
		}

		public void Init()
		{
		}

		protected string GetSetName(MessageType messageType)
		{
			return "nsb:subs:" + messageType.TypeName + ":" + messageType.Version;
		}

		public void Subscribe(Address subscriber, IEnumerable<MessageType> messageTypes)
		{

			Action subscribeAction = () =>
			{
				using (var client = GetClient())
				{
					using (var tran = client.CreateTransaction())
					{
						foreach (var messageType in messageTypes)
						{
							tran.QueueCommand(c => c.Sets[GetSetName(messageType)].Add(subscriber.ToString()));
						}
						tran.Commit();
					}
				}
			};

			if (Transaction.Current != null)
			{
				Transaction.Current.EnlistVolatile(new ActionResourceManager(subscribeAction, null), EnlistmentOptions.None);
			}
			else
			{
				subscribeAction();
			}
		}

		public void Unsubscribe(Address subscriber, IEnumerable<MessageType> messageTypes)
		{

			Action unsubscribeAction = () =>
			{
				using (var client = GetClient())
				{
					using (var tran = client.CreateTransaction())
					{
						foreach (var messageType in messageTypes)
						{
							tran.QueueCommand(c => c.Sets[GetSetName(messageType)].Remove(subscriber.ToString()));
						}
						tran.Commit();
					}
				}
			};

			if (Transaction.Current != null)
			{
				Transaction.Current.EnlistVolatile(new ActionResourceManager(unsubscribeAction, null), EnlistmentOptions.None);
			}
			else
			{
				unsubscribeAction();
			}
		}

		#endregion
	}
}
