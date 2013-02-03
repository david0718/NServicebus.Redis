using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using System.Web.Script.Serialization;
using log4net;
using NServiceBus;
using NServiceBus.Redis.Management;
using NServiceBus.Serialization;
using NServiceBus.Unicast.Queuing;
using NServiceBus.Unicast.Transport;
using ServiceStack.Redis;

namespace NServiceBus.Redis
{
	public class RedisQueue : ISendMessages, IReceiveMessages
	{
		protected Address _receiveAddress;
		protected bool _transactional;

		protected int _timeoutSeconds = 60;

		protected readonly IRedisClientsManager _clientManager;

		protected readonly ISerializer _serializer;

		protected readonly IQueueKeyNameProvider _keyNameProvider;

		protected readonly ILog _log;

		protected readonly QueueManager _manager;

		/// <summary>
		/// If true this will mean that the machine part of the address will not be used and only the logical endpoint name will be used in the
		/// queue name.
		/// </summary>
		protected bool _useSharedEndpointQueues = false;

		public RedisQueue(
			ISerializer serializer, 
			IRedisClientsManager clientManager, 
			IQueueKeyNameProvider keyNameProvider,
			int timeoutSeconds)
		{
			_timeoutSeconds = timeoutSeconds;
			_serializer = serializer;
			_clientManager = clientManager;
			_keyNameProvider = keyNameProvider;
			_log = log4net.LogManager.GetLogger(typeof(RedisQueue));
			_manager = new QueueManager(_serializer, _clientManager, _keyNameProvider);

			if (_timeoutSeconds == 0) _timeoutSeconds = 30;
			if (_serializer == null) throw new ArgumentNullException("serializer");
			if (_clientManager == null) throw new ArgumentNullException("clientManager");
			if (_keyNameProvider == null) throw new ArgumentNullException("keyNameProvider");
		}

		public RedisQueue(
			ISerializer serializer, 
			IRedisClientsManager clientManager,
			IQueueKeyNameProvider keyNameProvider
			)
			: this(serializer, clientManager, keyNameProvider, 60)
		{ }

		//TODO: Pluggable naming convention provider? Or maybe some config file options?
		//protected const string KeyPrefix = "nsb:queue:";

		protected string GetMessageIdQueueName(Address address)
		{
			return _keyNameProvider.GetMessageIdQueueName(address);
			//return GetBaseQueueName(address) + ":ids";
		}

		//protected string GetBaseQueueName(Address address)
		//{
		//	return _keyNameProvider.GetBaseQueueName(address);
		//	//if (_useSharedEndpointQueues) return KeyPrefix + address.Queue;
		//	//else return KeyPrefix + address.Queue + "@" + address.Machine;
		//}

		protected string GetCounterName(Address address)
		{
			return _keyNameProvider.GetCounterName(address);
			//return GetBaseQueueName(address) + ":counter";
		}

		protected string GetMessageHashName(Address address)
		{
			return _keyNameProvider.GetMessageHashName(address);
			//return GetBaseQueueName(address) + ":messages";
		}

		protected string GetClaimedMessageIdListName(Address address)
		{
			return _keyNameProvider.GetClaimedMessageIdListName(address);
			//return GetBaseQueueName(address) + ":claimed";
		}

		protected string GetMessageClaimTimeoutKey(Address address, string messageId)
		{
			return _keyNameProvider.GetMessageClaimTimeoutKey(address, messageId);
			//return GetBaseQueueName(address) + ":timeout:" + messageId;
		}

		protected RedisClient GetClient()
		{
			return _clientManager.GetClient() as RedisClient;
		}

		protected long IncrementId(Address address)
		{
			using (var client = GetClient())
			{
				return client.Increment(GetCounterName(address), 1);
			}
		}

		protected string Serialize(TransportMessage message)
		{
			return _serializer.SerializeToString(message);
		}

		protected TransportMessage Deserialize(string messageString)
		{
			return _serializer.DeserializeFromString<TransportMessage>(messageString);
		}

		/// <summary>
		/// Put an item back on the available queue
		/// </summary>
		/// <param name="messageId">The ID of the message being rolled back</param>
		internal void RollbackMessageReceive(string messageId)
		{
			if (_log.IsDebugEnabled) _log.Debug("Rolling back receive of message: " + _receiveAddress.ToString() + "/" + messageId);
			using (var client = GetClient())
			{
				//Show this put it back on the left or right of the queue?
				using (var tran = client.CreateTransaction())
				{
					tran.QueueCommand(c => c.RemoveItemFromList(GetClaimedMessageIdListName(_receiveAddress), messageId, -1)); //LREM
					tran.QueueCommand(c => c.Lists[GetMessageIdQueueName(_receiveAddress)].Prepend(messageId)); //LPUSH - maybe Append()/RPUSH instead?
					tran.QueueCommand(c => c.Remove(GetMessageClaimTimeoutKey(_receiveAddress, messageId))); //DEL the timeout
					tran.Commit();
				}
			}

			if (_log.IsDebugEnabled) _log.Debug("Rolled back receive of message: " + _receiveAddress.ToString() + "/" + messageId);
		}

		/// <summary>
		/// Delete an item once it's been successfully processed
		/// </summary>
		/// <param name="item"></param>
		internal void CommitMessageReceive(string messageId)
		{
			if (_log.IsDebugEnabled) _log.Debug("Committing receive of message: " + _receiveAddress.ToString() + "/" + messageId);
			using (var client = GetClient())
			{
				using (var tran = client.CreateTransaction())
				{
					tran.QueueCommand(c => c.RemoveItemFromList(GetClaimedMessageIdListName(_receiveAddress), messageId, -1)); //LREM the messageId
					tran.QueueCommand(c => c.Hashes[GetMessageHashName(_receiveAddress)].Remove(messageId)); //HDEL the message
					tran.QueueCommand(c => c.Remove(GetMessageClaimTimeoutKey(_receiveAddress, messageId))); //DEL the timeout
					tran.Commit();
				}
			}
			if (_log.IsDebugEnabled) _log.Debug("Committed receive of message: " + _receiveAddress.ToString() + "/" + messageId);
		}

		public void Send(TransportMessage message, Address address)
		{

			string messageId = Guid.NewGuid().ToString("N");
			message.Id = messageId;

			if (Transaction.Current != null)
			{
				if (_log.IsDebugEnabled) _log.Debug("Enlisting message send in transaction: " + address.ToString() + "/" + messageId);
				Transaction.Current.EnlistVolatile(new SendResourceManager(() => DoSend(message, address)), EnlistmentOptions.None); //Pass new instance?
			}
			else
			{
				DoSend(message, address);
			}
		}

		internal void DoSend(TransportMessage message, Address address)
		{
			using (var client = GetClient())
			{
				string hashName = GetMessageHashName(address);
				string queueName = GetMessageIdQueueName(address);
				string messageId = message.Id;
				
				string serializedMessage = Serialize(message);

				if (_log.IsDebugEnabled) _log.Debug("Sending message: " + address.ToString() + "/" + messageId);

				using (var tran = client.CreateTransaction())
				{
					tran.QueueCommand(c => c.Hashes[hashName].AddIfNotExists(new KeyValuePair<string, string>(messageId, serializedMessage))); //HSETNX
					tran.QueueCommand(c => c.Lists[queueName].Prepend(messageId)); //LPUSH 	
					tran.Commit();
				}

				if (_log.IsDebugEnabled) _log.Debug("Sent message: " + address.ToString() + "/" + messageId);
			}

		}

		public void Init(Address address, bool transactional)
		{
			if (_log.IsDebugEnabled) _log.Debug("Initializing message receiver: " + address.ToString());
			
			_receiveAddress = address;
			_transactional = transactional;

			//Schedule expiry. Will only work in the host
			if (NServiceBus.Configure.Instance != null)
			{
				if (_log.IsDebugEnabled) _log.Debug("Scheduling claim expiry: " + address.ToString());
				NServiceBus.Schedule.Every(TimeSpan.FromSeconds(30)).Action("ExpireClaimed", () => 
				{
					_manager.ExpireClaimedMessages(_receiveAddress);
				});
			}
			
		}

		public bool HasMessage()
		{
			return true; //We are using a blocking call to receive so this can just return true;
		}

		public TransportMessage Receive()
		{
			using (var client = GetClient())
			{
				TransportMessage message = null;

				if (_log.IsDebugEnabled) _log.Debug("Waiting to receive message: " + _receiveAddress.ToString());
				//Use blocking call to retreive message
				string messageId = client.BlockingPopAndPushItemBetweenLists(GetMessageIdQueueName(_receiveAddress), GetClaimedMessageIdListName(_receiveAddress), new TimeSpan(0,0,30));
				if (messageId != null)
				{
					if (_log.IsDebugEnabled) _log.Debug("Received message: " + _receiveAddress.ToString() + "/" + messageId);
					
					string serializedMessage = client.Hashes[GetMessageHashName(_receiveAddress)][messageId];
					if (serializedMessage != null)
					{
						if (_log.IsDebugEnabled) _log.Debug("Retrieved message data: " + _receiveAddress.ToString() + "/" + messageId);
						
						if (_transactional && Transaction.Current != null)
						{
							if (_log.IsDebugEnabled) _log.Debug("Enlisting in transaction: " + _receiveAddress.ToString() + "/" + messageId);
						
							Transaction.Current.EnlistVolatile(new ReceiveResourceManager(messageId, this), EnlistmentOptions.None); //Pass new instance?
							//Create a timeout for the received message
							client.Set(GetMessageClaimTimeoutKey(_receiveAddress, messageId), DateTime.UtcNow.ToString("u"), TimeSpan.FromSeconds(_timeoutSeconds));
						}
						else
						{
							//Not transactional to commit the receive straight away
							CommitMessageReceive(messageId);
						}

						message = Deserialize(serializedMessage);

						if (_log.IsDebugEnabled) _log.Debug("Deserialized message: " + _receiveAddress.ToString() + "/" + messageId);
					}
				}

				return message;
			}
		}
	}
}
