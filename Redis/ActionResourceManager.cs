using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;

namespace NServiceBus.Redis
{
	internal class ActionResourceManager : IEnlistmentNotification
	{

		private readonly Action _commitAction;
		private readonly Action _rollbackAction; 

		public ActionResourceManager(Action commitAction, Action rollbackAction)
		{
			_commitAction = commitAction;
			_rollbackAction = rollbackAction;
		}

		#region IEnlistmentNotification Members

		public void Commit(Enlistment enlistment)
		{
			if (_commitAction != null) _commitAction();
			enlistment.Done();
		}

		public void InDoubt(Enlistment enlistment)
		{
			if (_rollbackAction != null) _rollbackAction();
			enlistment.Done();
		}

		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			preparingEnlistment.Prepared();
		}

		public void Rollback(Enlistment enlistment)
		{
			if (_rollbackAction != null) _rollbackAction();
			enlistment.Done();
		}

		#endregion
	}
}
