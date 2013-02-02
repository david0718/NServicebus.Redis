using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus;
using NServiceBus.Unicast.Transport;

namespace NServiceBus.Redis
{
	class SendResourceManager : IEnlistmentNotification
	{

		private TransportMessage _message;
		private Address _address;
		private RedisQueue _queue;
		private Action _actionOnCommit;

		public SendResourceManager(TransportMessage message, Address address, RedisQueue queue)
		{
			_message = message;
			_address = address;
			_queue = queue;
		}

		public SendResourceManager(Action actionOnCommit)
		{
			_actionOnCommit = actionOnCommit;
		}

		#region IEnlistmentNotification Members

		public void Commit(Enlistment enlistment)
		{
			if (_actionOnCommit != null) _actionOnCommit();
			else if(_queue != null) _queue.DoSend(_message, _address);
			enlistment.Done();
		}

		public void InDoubt(Enlistment enlistment)
		{
			enlistment.Done();
		}

		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			preparingEnlistment.Prepared();
		}

		public void Rollback(Enlistment enlistment)
		{
			enlistment.Done();
		}

		#endregion
	}
}
