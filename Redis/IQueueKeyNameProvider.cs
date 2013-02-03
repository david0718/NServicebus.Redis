using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NServiceBus.Redis
{
	public interface IQueueKeyNameProvider
	{

		bool UseSharedQueues { get; }

		string KeyPrefix { get; }

		string GetBaseQueueName(Address address);
		string GetMessageIdQueueName(Address address);
		string GetCounterName(Address address);
		string GetMessageHashName(Address address);
		string GetClaimedMessageIdListName(Address address);
		string GetMessageClaimTimeoutKey(Address address, string messageId);

		Address GetQueueAddressFromKey(string key);

		string GetKeySearchPattern();
	}
}
