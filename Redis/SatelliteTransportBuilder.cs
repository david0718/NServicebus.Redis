using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NServiceBus.Faults;
using NServiceBus.ObjectBuilder;
using NServiceBus.Satellites;
using NServiceBus.Unicast.Transport;
using NServiceBus.Unicast.Transport.Transactional;
using ServiceStack.Redis;

namespace NServiceBus.Redis
{
	public class SatelliteTransportBuilder : ISatelliteTransportBuilder
	{
		public IBuilder Builder { get; set; }
		public TransactionalTransport MainTransport { get; set; }

		/// <summary>
		/// This requires RedisQueue to be NOT register as a single instance! It's lightweight though so no real issue.
		/// </summary>
		public RedisQueue Queue { get; set; }

		//public IRedisClientsManager ClientManager { get; set; }

		public ITransport Build()
		{
			//var nt = 1; // MainTransport != null ? MainTransport.NumberOfWorkerThreads == 0 ? 1 : MainTransport.NumberOfWorkerThreads : 1;
			var nt = MainTransport != null ? MainTransport.NumberOfWorkerThreads == 0 ? 1 : MainTransport.NumberOfWorkerThreads : 1;
			var mr = MainTransport != null ? MainTransport.MaxRetries : 1;
			var tx = MainTransport != null ? MainTransport.IsTransactional : true;

			var fm = MainTransport != null
						 ? Builder.Build(MainTransport.FailureManager.GetType()) as IManageMessageFailures
						 : Builder.Build<IManageMessageFailures>();

			return new TransactionalTransport
			{
				MessageReceiver = Queue,
				IsTransactional = tx,
				NumberOfWorkerThreads = nt,
				MaxRetries = mr,
				FailureManager = fm
			};
		}
	}
}
