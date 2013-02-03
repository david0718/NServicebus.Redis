using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using NServiceBus.Unicast.Transport;
using ServiceStack.Redis;

namespace NServiceBus.Redis.Management
{
	public class QueueManager
	{
		protected IRedisClientsManager _clientManager;
		protected ISerializer _serializer;
		protected readonly IQueueKeyNameProvider _keyNameProvider;
		protected ILog _log;

		public QueueManager(
			ISerializer serializer, 
			IRedisClientsManager clientManager, 
			IQueueKeyNameProvider keyNameProvider)
		{
			_serializer = serializer;
			_clientManager = clientManager;
			_keyNameProvider = keyNameProvider;
			_log = log4net.LogManager.GetLogger(typeof(QueueManager));
		}

		protected RedisClient GetClient()
		{
			return _clientManager.GetClient() as RedisClient;
		}

		protected string Serialize(TransportMessage message)
		{
			return _serializer.SerializeToString(message);
		}

		protected TransportMessage Deserialize(string messageString)
		{
			return _serializer.DeserializeFromString<TransportMessage>(messageString);
		}

		public IList<Address> GetAllQueues()
		{
			var queues = new List<Address>();

			using (var client = GetClient())
			{
				var queueKeys = client.SearchKeys(_keyNameProvider.GetKeySearchPattern());

				if (queueKeys != null)
				{
					foreach (var queueKey in queueKeys)
					{
						var queueAddress = _keyNameProvider.GetQueueAddressFromKey(queueKey);
						if (queueAddress != null)
						{
							var messageCount = GetMessageCount(queueAddress);
							queues.Add(queueAddress);
						}
					}
				}
			}

			return queues;
		}

		public bool DeleteQueue(Address address)
		{
			using (var client = GetClient())
			{

				bool exists = client.Exists(_keyNameProvider.GetMessageIdQueueName(address)) > 0;

				using (var tran = client.CreateTransaction())
				{
					tran.QueueCommand(c => c.Remove(_keyNameProvider.GetClaimedMessageIdListName(address))); //LREM the messageId
					tran.QueueCommand(c => c.Remove(_keyNameProvider.GetMessageHashName(address))); //LREM the messageId
					tran.QueueCommand(c => c.Remove(_keyNameProvider.GetMessageIdQueueName(address))); //LREM the messageId

					tran.Commit();
				}

				return exists;
			}
		}

		public void SendMessageToQueue(TransportMessage message, Address address)
		{

			if (string.IsNullOrEmpty(message.Id)) message.Id = Guid.NewGuid().ToString("N");

			using (var client = GetClient())
			{
				string hashName = _keyNameProvider.GetMessageHashName(address);
				string queueName = _keyNameProvider.GetMessageIdQueueName(address);
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

		public void DeleteMessageFromQueue(string messageId, Address address)
		{
			using (var client = GetClient())
			{	
				if (_log.IsDebugEnabled) _log.Debug("Deleting message: " + address.ToString() + "/" + messageId);

				using (var tran = client.CreateTransaction())
				{
					tran.QueueCommand(c => c.RemoveItemFromList(_keyNameProvider.GetClaimedMessageIdListName(address), messageId, -1)); //LREM the messageId
					tran.QueueCommand(c => c.Remove(_keyNameProvider.GetMessageClaimTimeoutKey(address, messageId))); //DEL the timeout
					tran.QueueCommand(c => c.Hashes[_keyNameProvider.GetMessageHashName(address)].Remove(messageId)); //HDEL the message
					tran.QueueCommand(c => c.RemoveItemFromList(_keyNameProvider.GetMessageIdQueueName(address), messageId, -1)); //LREM the messageId
						
					tran.Commit();
				}

				if (_log.IsDebugEnabled) _log.Debug("Deleted message: " + address.ToString() + "/" + messageId);
			}

		}

		public int GetMessageCount(Address address)
		{
			using (var client = GetClient())
			{
				return client.Lists[_keyNameProvider.GetMessageIdQueueName(address)].Count;
			}
		}

		public void ReturnAllMessagesToSourceQueue(Address address)
		{
			using (var client = GetClient())
			{
				if (_log.IsDebugEnabled) _log.Debug("Acquiring lock");
				using (var rlock = client.AcquireLock("lock:" + address.ToString(), TimeSpan.FromMinutes(1)))
				{
					if (_log.IsDebugEnabled) _log.Debug("Lock acquired");

					if (_log.IsDebugEnabled) _log.Debug("Retrieveing all message IDs from " + address.ToString());
					var messageIds = client.Lists[_keyNameProvider.GetMessageIdQueueName(address)].GetAll();

					if (messageIds != null)
					{
						foreach (var messageId in messageIds)
						{
							ReturnMessageToSourceQueue(address, messageId);
						}
					}
				}
			}
		}

		public IList<TransportMessage> GetAllMessages(Address address)
		{
			IList<TransportMessage> messages = new List<TransportMessage>();

			string messageIdQueueName = _keyNameProvider.GetMessageIdQueueName(address);

			using (var client = GetClient())
			{
				var serializedMessages = client.GetHashValues(_keyNameProvider.GetMessageHashName(address));
				if (serializedMessages != null)
				{
					foreach (var serializedMessage in serializedMessages)
					{
						messages.Add(Deserialize(serializedMessage));
					}
				}
			}

			return messages;
		}

		public TransportMessage GetMessageById(Address address, string messageId)
		{
			TransportMessage message = null;

			string messageIdQueueName = _keyNameProvider.GetMessageIdQueueName(address);

			using (var client = GetClient())
			{
				string serializedMessage = client.Hashes[_keyNameProvider.GetMessageHashName(address)][messageId];
				if (serializedMessage != null)
				{
					message = Deserialize(serializedMessage);
				}
			}

			return message;
		}

		protected void MoveMessage(Address sourceAddress, Address targetAddress, string messageId)
		{
			var message = GetMessageById(sourceAddress, messageId);

			if (message != null)
			{
				var serializedMessage = Serialize(message);

				using (var client = GetClient())
				{
					using (var tran = client.CreateTransaction())
					{
						//Delete from source queue:
						tran.QueueCommand(c => c.RemoveItemFromList(_keyNameProvider.GetClaimedMessageIdListName(sourceAddress), messageId, -1)); //LREM the messageId
						tran.QueueCommand(c => c.Remove(_keyNameProvider.GetMessageClaimTimeoutKey(sourceAddress, messageId))); //DEL the timeout
						tran.QueueCommand(c => c.Hashes[_keyNameProvider.GetMessageHashName(sourceAddress)].Remove(messageId)); //HDEL the message
						tran.QueueCommand(c => c.RemoveItemFromList(_keyNameProvider.GetMessageIdQueueName(sourceAddress), messageId, -1)); //LREM the messageId

						//Add to targetQueue:
						tran.QueueCommand(c => c.Hashes[_keyNameProvider.GetMessageHashName(targetAddress)].AddIfNotExists(new KeyValuePair<string, string>(messageId, serializedMessage))); //HSETNX
						tran.QueueCommand(c => c.Lists[_keyNameProvider.GetMessageIdQueueName(targetAddress)].Prepend(messageId)); //LPUSH 	

						tran.Commit();
					}
				}
			}
		}

		public void ReturnMessageToSourceQueue(Address address, string messageId)
		{

			if (_log.IsDebugEnabled) _log.Debug("Retrieving message: " + address.ToString() + "/" + messageId);

			var message = GetMessageById(address, messageId);
			if (message != null)
			{
				string failedQueueName = null;
				if (message.Headers.ContainsKey(Faults.FaultsHeaderKeys.FailedQ))
				{
					failedQueueName = message.Headers[Faults.FaultsHeaderKeys.FailedQ];
				}
				
				if(string.IsNullOrEmpty(failedQueueName))
				{
					throw new ApplicationException("Message does not have a header indicating from which queue it came: " + messageId);
				}

				var targetAddress = Address.Parse(failedQueueName);

				if (_log.IsDebugEnabled) _log.Debug("Failed queue name: " + targetAddress.ToString());

				var serializedMessage = Serialize(message);

				if (_log.IsDebugEnabled) _log.Debug(string.Format("Moving message {0} from {1} to {2}...", messageId, address.ToString(), targetAddress.ToString()));
				using (var client = GetClient())
				{
					using (var tran = client.CreateTransaction())
					{
						//Delete from source queue:
						tran.QueueCommand(c => c.RemoveItemFromList(_keyNameProvider.GetClaimedMessageIdListName(address), messageId, -1)); //LREM the messageId
						tran.QueueCommand(c => c.Remove(_keyNameProvider.GetMessageClaimTimeoutKey(address, messageId))); //DEL the timeout
						tran.QueueCommand(c => c.Hashes[_keyNameProvider.GetMessageHashName(address)].Remove(messageId)); //HDEL the message
						tran.QueueCommand(c => c.RemoveItemFromList(_keyNameProvider.GetMessageIdQueueName(address), messageId, -1)); //LREM the messageId
						
						//Add to targetQueue:
						tran.QueueCommand(c => c.Hashes[_keyNameProvider.GetMessageHashName(targetAddress)].AddIfNotExists(new KeyValuePair<string, string>(messageId, serializedMessage))); //HSETNX
						tran.QueueCommand(c => c.Lists[_keyNameProvider.GetMessageIdQueueName(targetAddress)].Prepend(messageId)); //LPUSH 	

						tran.Commit();
					}
				}
				if (_log.IsDebugEnabled) _log.Debug(string.Format("Moved message {0} from {1} to {2}", messageId, address.ToString(), targetAddress.ToString()));
	
			}
		}

		/// <summary>
		/// Expires claimed messages that have exceeded the timeout time and pushes them on the back of the queue again
		/// </summary>
		/// <param name="address">Address of the queue to expire</param>
		/// <returns>Number of messages that were expired</returns>
		public int ExpireClaimedMessages(Address address)
		{
			if (_log.IsDebugEnabled) _log.Debug("Expiring claimed messages for address: " + address.ToString());
			using (var client = GetClient())
			{
				int expired = 0;
				string claimedListName = _keyNameProvider.GetClaimedMessageIdListName(address);
				string queueName = _keyNameProvider.GetMessageIdQueueName(address);
				foreach (var messageId in client.Lists[claimedListName].GetAll())
				{
					if (client.Exists(_keyNameProvider.GetMessageClaimTimeoutKey(address, messageId)) == 0)
					{
						using (var tran = client.CreateTransaction())
						{
							tran.QueueCommand(c => c.RemoveItemFromList(claimedListName, messageId, -1)); //LREM
							tran.QueueCommand(c => c.Lists[queueName].Prepend(messageId)); //LPUSH	
							tran.Commit();
						}
						expired++;
					}
				}
				if (_log.IsDebugEnabled) _log.Debug("Expired " + expired + " claimed messages for address: " + address.ToString());
				return expired;
			}
		}
	}
}
