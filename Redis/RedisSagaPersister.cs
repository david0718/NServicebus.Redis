using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus.Saga;
using ServiceStack.Redis;

namespace NServiceBus.Redis
{
	public class RedisSagaPersister : ISagaPersister
	{

		protected IRedisClientsManager _clientManager;
		protected ISerializer _serializer;

		protected const string KeyPrefix = "nsb:";

		public RedisSagaPersister(ISerializer serializer, IRedisClientsManager clientManager)
		{
			_clientManager = clientManager;
			_serializer = serializer;
		}

		protected RedisClient GetClient()
		{
			return _clientManager.GetClient() as RedisClient;
		}

		protected bool IsUniqueProperty<T>(string property)
		{
			return UniqueAttribute.GetUniqueProperties(typeof(T)).FirstOrDefault(p => p.Name == property) != null;
		}

		protected string GetPropertyKey(Type sagaType, string propertyName, object value)
		{
			var valueString = StringifyObject(value);
			if (valueString == null) throw new ArgumentNullException("Unique property " + propertyName + " has a value of null");
			return KeyPrefix + "saga:property:" + sagaType.FullName + ":" + propertyName + ":" + valueString;
		}

		protected string StringifyObject(object value)
		{
			if (value == null) return null;
			var valueStr = value.ToString();

			//Generate GUID from 16 byte hash
			var hasher = new System.Security.Cryptography.MD5CryptoServiceProvider();
			var hash = hasher.ComputeHash(Encoding.UTF8.GetBytes(valueStr));

			var guid = new Guid(hash);

			return guid.ToString("N");
		}

		protected string GetSagaKey(ISagaEntity saga)
		{
			if (saga == null) throw new ArgumentNullException("saga");
			return KeyPrefix + "saga:" + saga.GetType().FullName + ":" + saga.Id.ToString("N");
		}

		protected string GetSagaKey<T>(Guid sagaId)
		{
			return KeyPrefix + "saga:" + typeof(T).FullName + ":" + sagaId.ToString("N");
		}

		protected string GetSagaVersionKey(ISagaEntity saga)
		{
			return GetSagaKey(saga) + ":version";
		}

		protected string GetSagaPropertyMapKey(ISagaEntity saga)
		{
			return GetSagaKey(saga) + ":map";
		}

		protected string Serialize(ISagaEntity saga)
		{
			return _serializer.SerializeToString(saga);
		}

		protected T Deserialize<T>(string value)
		{
			return _serializer.DeserializeFromString<T>(value);
		}

		protected long GetSagaVersion(ISagaEntity saga)
		{
			var versionProp = saga.GetType().GetProperties().FirstOrDefault(o => o.Name == "Version");

			if (versionProp == null) throw new MissingMemberException("'Version' property must be defined on saga data");

			return Convert.ToInt64(versionProp.GetValue(saga, null));
		}

		protected void SetSagaVersion(ISagaEntity saga, long version)
		{
			var versionProp = saga.GetType().GetProperties().FirstOrDefault(o => o.Name == "Version");

			if (versionProp == null) throw new MissingMemberException("'Version' property must be defined");

			versionProp.SetValue(saga, version, null);
		}

		#region ISagaPersister Members

		public void Complete(ISagaEntity saga)
		{
			DeleteSaga(saga);
		}

		public T Get<T>(string property, object value) where T : ISagaEntity
		{
			var propertyKey = GetPropertyKey(typeof(T), property, value);

			using (var client = GetClient())
			{
				var sagaId = client.Get<string>(propertyKey);
				if (sagaId != null)
				{
					var sagaKey = GetSagaKey<T>(Guid.Parse(sagaId));
					var sagaString = client.Get<string>(sagaKey);
					if (sagaString != null)
					{
						var saga = Deserialize<T>(sagaString);
						return saga;
					}
				}
			}

			return default(T);
		}

		public T Get<T>(Guid sagaId) where T : ISagaEntity
		{
			var sagaKey = GetSagaKey<T>(sagaId);

			using (var client = GetClient())
			{
				var sagaString = client.Get<string>(sagaKey);
				if (sagaString != null)
				{
					var saga = Deserialize<T>(sagaString);
					return saga;
				}
			}

			return default(T);
		}

		public void Save(ISagaEntity saga)
		{
			var stringId = saga.Id.ToString("N");

			var sagaKey = GetSagaKey(saga);
			var sagaPropertyMapKey = GetSagaPropertyMapKey(saga);
			var sagaVersionKey = GetSagaVersionKey(saga);

			var uniqueProperties = UniqueAttribute.GetUniqueProperties(saga);

			Action saveSagaAction = () =>
			{
				using (var client = GetClient())
				{
					long version = client.Increment(sagaVersionKey, 1);

					SetSagaVersion(saga, version);

					var sagaString = Serialize(saga);

					//Check that unique properties don't already exist for a different saga
					foreach (var prop in uniqueProperties)
					{
						var propertyKey = GetPropertyKey(saga.GetType(), prop.Key, prop.Value);
						var sagaId = client.Get<string>(propertyKey);
						if (sagaId != null)
						{
							if (saga.Id != Guid.Parse(sagaId))
							{
								throw new UniquePropertyException("Unique property " + prop.Key + " already exists with value " + prop.Value);
							}
						}
					}

					using (var tran = client.CreateTransaction())
					{
						tran.QueueCommand(c => c.Set(sagaKey, sagaString));
						foreach (var prop in uniqueProperties)
						{
							var propertyKey = GetPropertyKey(saga.GetType(), prop.Key, prop.Value);
							tran.QueueCommand(c => c.Lists[sagaPropertyMapKey].Add(propertyKey)); //Link from saga ID to property key
							tran.QueueCommand(c => client.Set(propertyKey, stringId));  //Link from property key to saga
						}
						tran.Commit();
					}
				}
			};

			if (Transaction.Current != null)
			{
				Transaction.Current.EnlistVolatile(new ActionResourceManager(saveSagaAction, null), EnlistmentOptions.None);
			}
			else
			{
				saveSagaAction();
			}
		}

		protected void DeleteSaga(ISagaEntity saga)
		{
			var stringId = saga.Id.ToString("N");

			var sagaKey = GetSagaKey(saga);
			var sagaPropertyMapKey = GetSagaPropertyMapKey(saga);
			var sagaVersionKey = GetSagaVersionKey(saga);

			Action deleteSagaAction = () =>
			{
				using (var client = GetClient())
				{
					var propertyKeys = client.Lists[sagaPropertyMapKey].GetAll();

					using (var tran = client.CreateTransaction())
					{
						if (propertyKeys != null)
						{
							tran.QueueCommand(c => c.RemoveAll(propertyKeys.ToArray()));
						}
						tran.QueueCommand(c => c.Remove(sagaKey));
						tran.QueueCommand(c => c.Remove(sagaPropertyMapKey));
						tran.QueueCommand(c => c.Remove(sagaVersionKey));
						tran.Commit();
					}
				}
			};

			if (Transaction.Current != null)
			{
				Transaction.Current.EnlistVolatile(new ActionResourceManager(deleteSagaAction, null), EnlistmentOptions.None);
			}
			else
			{
				deleteSagaAction();
			}
		}

		public void Update(ISagaEntity saga)
		{
			long currentVersion = GetSagaVersion(saga);
			var sagaKey = GetSagaKey(saga);
			var sagaVersionKey = GetSagaVersionKey(saga);

			using (var client = GetClient())
			{
				var versionString = client.Get<string>(sagaVersionKey);
				if (versionString != null)
				{
					long storageVersion = long.Parse(versionString);

					if (storageVersion != currentVersion) throw new ConcurrencyException("The saga " + saga.Id + " has changed since it was last loaded");
				}
				else
				{
					throw new ConcurrencyException("The saga " + saga.Id + " has been deleted since it was last loaded");
				}
			}

			Save(saga);
		}

		#endregion
	}
}
