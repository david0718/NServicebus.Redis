using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using NServiceBus.Timeout.Core;
using ServiceStack.Redis;

namespace NServiceBus.Redis
{
	public class RedisTimeoutPersistence : IPersistTimeouts
	{

		protected IRedisClientsManager _clientManager;
		protected string _endpointName;
		protected ILog _log;
		protected ISerializer _serializer;

		public string EndpointName
		{
			get
			{
				return _endpointName;
			}
			set
			{
				_endpointName = value;
			}
		}

		public RedisTimeoutPersistence(ISerializer serializer, IRedisClientsManager clientManager)
		{
			_clientManager = clientManager;
			_log = log4net.LogManager.GetLogger(typeof(RedisTimeoutPersistence));
			_serializer = serializer;
		}

		protected RedisClient GetClient()
		{
			return _clientManager.GetClient() as RedisClient;
		}

		protected string Serialize(TimeoutData timeout)
		{
			return _serializer.SerializeToString(timeout);
		}

		protected TimeoutData Deserialize(string data)
		{
			return _serializer.DeserializeFromString<TimeoutData>(data);
		}

		protected DateTime GetStartDate()
		{
			return new DateTime(2000, 1, 1);
		}

		protected int ScoreDateTime(DateTime time)
		{
			return (int)time.Subtract(GetStartDate()).TotalSeconds;
		}

		protected DateTime GetDateTimeFromScore(int score)
		{
			return GetStartDate().AddSeconds(score);
		}

		protected string GetKeyPrefix()
		{
			return "nsb:timeouts:" + _endpointName;
		}

		protected string GetSagaIdMapHashName()
		{
			return GetKeyPrefix() + ":sagaidmap";
		}

		protected string GetTimeoutIdsListName()
		{
			return GetKeyPrefix() + ":ids";
		}

		protected string GetTimeoutDataHashName()
		{
			return GetKeyPrefix() + ":data";
		}

		protected string GetTimeoutTimesSortedSetName()
		{
			return GetKeyPrefix() + ":times";
		}

		#region IPersistTimeouts Members

		public void Add(TimeoutData timeout)
		{
			timeout.Id = Guid.NewGuid().ToString("N");

			int score = ScoreDateTime(timeout.Time);

			if(_log.IsDebugEnabled) _log.Debug("Adding timeout with ID " + timeout.Id + " " + timeout.Time.ToString() + " " + score);

			using (var client = GetClient())
			{
				var nativeClient = client as RedisNativeClient;
				using (var tran = client.CreateTransaction())
				{
					tran.QueueCommand(c => c.Lists[GetTimeoutIdsListName()].Add(timeout.Id));
					tran.QueueCommand(c => c.Hashes[GetTimeoutDataHashName()].AddIfNotExists(new KeyValuePair<string, string>(timeout.Id, Serialize(timeout))));
					tran.QueueCommand(c => ((RedisNativeClient)c).ZAdd(GetTimeoutTimesSortedSetName(), score, Encoding.UTF8.GetBytes(timeout.Id)));
					if (timeout.SagaId != Guid.Empty)
					{
						tran.QueueCommand(c => c.Hashes[GetSagaIdMapHashName()].AddIfNotExists(new KeyValuePair<string, string>(timeout.SagaId.ToString("N"), timeout.Id)));
					}
					tran.Commit();
				}
			}
		}

		public List<Tuple<string, DateTime>> GetNextChunk(DateTime startSlice, out DateTime nextTimeToRunQuery)
		{
			var timeouts = new List<Tuple<string,DateTime>>();

			var now = DateTime.UtcNow;

			int startScore = ScoreDateTime(startSlice);
			int nowScore = ScoreDateTime(now);

			using (var client = GetClient())
			{
				var timeoutIds = client.ZRangeByScoreWithScores(GetTimeoutTimesSortedSetName(), startScore, nowScore, null, null);

				bool isValue = true;
				string currentTimeoutId = null;
				int currentScore = 0;
				foreach(byte[] item in timeoutIds)
				{
					if (isValue)
					{
						currentTimeoutId = Encoding.UTF8.GetString(item);
					}
					else
					{
						currentScore = int.Parse(Encoding.UTF8.GetString(item));
						timeouts.Add(new Tuple<string, DateTime>(currentTimeoutId, GetDateTimeFromScore(currentScore)));
					}
					isValue = !isValue;
				}

				//TODO: Another query to look up future timeouts?
				nextTimeToRunQuery = DateTime.UtcNow.AddMinutes(1);
				return timeouts;
			}
		}

		public void RemoveTimeoutBy(Guid sagaId)
		{
			var stringId = sagaId.ToString("N");

			using (var client = GetClient())
			{
				var timeoutId = client.Hashes[GetSagaIdMapHashName()][stringId];

				if (timeoutId != null)
				{
					using (var tran = client.CreateTransaction())
					{
						tran.QueueCommand(c => c.Lists[GetTimeoutIdsListName()].Remove(timeoutId));
						tran.QueueCommand(c => c.Hashes[GetTimeoutDataHashName()].Remove(timeoutId));
						tran.QueueCommand(c => c.SortedSets[GetTimeoutTimesSortedSetName()].Remove(timeoutId));
						tran.QueueCommand(c => c.Hashes[GetSagaIdMapHashName()].Remove(stringId));
						tran.Commit();
					}
				}				
			}
		}

		public bool TryRemove(string timeoutId, out TimeoutData timeoutData)
		{
			using (var client = GetClient())
			{
				var timeoutString = client.Hashes[GetTimeoutDataHashName()][timeoutId];

				if (timeoutString != null)
				{
					var myTimeoutData = Deserialize(timeoutString);

					using (var tran = client.CreateTransaction())
					{
						tran.QueueCommand(c => c.Lists[GetTimeoutIdsListName()].Remove(timeoutId));
						tran.QueueCommand(c => c.Hashes[GetTimeoutDataHashName()].Remove(timeoutId));
						tran.QueueCommand(c => c.SortedSets[GetTimeoutTimesSortedSetName()].Remove(timeoutId));
						tran.QueueCommand(c => c.Hashes[GetSagaIdMapHashName()].Remove(myTimeoutData.SagaId.ToString("N")));
						tran.Commit();
					}

					timeoutData = myTimeoutData;

					return true;
				}
				else
				{
					timeoutData = null;
					return false;
				}
			}
		}

		#endregion
	}
}
