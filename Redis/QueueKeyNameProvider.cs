using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NServiceBus.Redis
{
	public class QueueKeyNameProvider : IQueueKeyNameProvider
	{

		public bool UseSharedQueues { get; private set; }

		public string KeyPrefix { get; private set; }

		public QueueKeyNameProvider(bool useSharedQueues) : this("nsb:queue:", useSharedQueues) { }

		public QueueKeyNameProvider(string prefix, bool useSharedQueues)
		{
			KeyPrefix = prefix;
			UseSharedQueues = useSharedQueues;
		}

		public virtual string GetMessageIdQueueName(Address address)
		{
			return GetBaseQueueName(address) + ":ids";
		}

		public virtual string GetBaseQueueName(Address address)
		{
			if (UseSharedQueues) return KeyPrefix + address.Queue;
			else return KeyPrefix + address.Queue + "@" + address.Machine;
		}

		public virtual string GetCounterName(Address address)
		{
			return GetBaseQueueName(address) + ":counter";
		}

		public virtual string GetMessageHashName(Address address)
		{
			return GetBaseQueueName(address) + ":messages";
		}

		public virtual string GetClaimedMessageIdListName(Address address)
		{
			return GetBaseQueueName(address) + ":claimed";
		}

		public virtual string GetMessageClaimTimeoutKey(Address address, string messageId)
		{
			return GetBaseQueueName(address) + ":timeout:" + messageId;
		}

		public virtual Address GetQueueAddressFromKey(string key)
		{
			var parts = key.Split(':');
			if (parts.Length >= 3)
			{
				return Address.Parse(parts[2]);
			}
			return null;
		}

		public string GetKeySearchPattern()
		{
			return KeyPrefix + "*:ids";
		}

	}
}
