using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace NServiceBus.Redis
{
	//TODO: Do transactional stuff!
	internal class ReceiveResourceManager : IEnlistmentNotification
	{

		private string _messageId;
		private RedisQueue _queue;

		public ReceiveResourceManager(string messageId, RedisQueue queue)
		{
			_messageId = messageId;
			_queue = queue;
		}

		#region IEnlistmentNotification Members

		public void Commit(Enlistment enlistment)
		{
			_queue.CommitMessageReceive(_messageId);
			enlistment.Done();
		}

		public void InDoubt(Enlistment enlistment)
		{
			_queue.RollbackMessageReceive(_messageId); //Is this correct behavior? Check this!
			enlistment.Done();
		}

		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			preparingEnlistment.Prepared();
		}

		public void Rollback(Enlistment enlistment)
		{
			_queue.RollbackMessageReceive(_messageId);
			enlistment.Done();
		}

		#endregion
	}
}
