using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus;
using NUnit.Framework;
using NServiceBus.Redis;
using ServiceStack.Redis;
using NServiceBus.Unicast.Subscriptions;
using NServiceBus.Timeout.Core;
using NServiceBus.Saga;
using NServiceBus.Serializers.Json;
using NServiceBus.MessageInterfaces.MessageMapper.Reflection;
using NServiceBus.Unicast.Transport;
using System.IO;
using NServiceBus.Redis.Management;

namespace Tests
{
	/*
	 * TODO: Write more comprehensive tests around concurrency etc
	 */

	[TestFixture]
	public class RedisTests
	{
		//private IBus _bus;
		
		public RedisTests()
		{
			//bus = GetBus();
		}

		private IBus GetBus()
		{
			return Configure.With()
				.DefineEndpointName("test")
				.DefaultBuilder()
				.Log4Net()
				.JsonSerializer()
				.UnicastBus()
				.IsTransactional(true)
				.RedisForEverything(true, "localhost")
				.CreateBus()
				.Start();

		}

		[Test]
		public void Test_Subscription_Storage_Store_And_Retrieve()
		{
			var clientManager = new PooledRedisClientManager();

			var store = new RedisSubscriptionStorage(new JsonSerializer(), clientManager);

			clientManager.GetClient().FlushDb();

			var addr = new Address("test", "test");

			var messageTypes = new[] { new MessageType(typeof(TestMessage)) };

			store.Subscribe(addr, messageTypes);

			var subscribers = store.GetSubscriberAddressesForMessage(messageTypes);

			Assert.IsTrue(subscribers.Count() > 0 && subscribers.First().Queue == "test" && subscribers.First().Machine == "test");

			store.Unsubscribe(addr, messageTypes);

			var subscribers2 = store.GetSubscriberAddressesForMessage(messageTypes);

			Assert.IsEmpty(subscribers2);

		}

		[Test]
		public void Test_Timeout_Persistence_RemoveTimeoutBy_Works()
		{
			var clientManager = new PooledRedisClientManager();

			var store = new RedisTimeoutPersistence(new JsonSerializer(), clientManager);
			store.EndpointName = "timeouttest";

			clientManager.GetClient().FlushDb();
			
			var data = new TimeoutData()
			{
				SagaId = Guid.NewGuid(),
				Time = DateTime.UtcNow.AddSeconds(-30),
				OwningTimeoutManager = store.EndpointName,
				Destination = Address.Parse("test@localhost"),
			};

			store.Add(data);

			store.RemoveTimeoutBy(data.SagaId);

			DateTime nextRun;
			var timeouts = store.GetNextChunk(DateTime.UtcNow.AddMinutes(-2), out nextRun);

			Assert.IsEmpty(timeouts);
		
		}

		[Test]
		public void Test_Timeout_Persistence_TryRemove_Works()
		{

			var clientManager = new PooledRedisClientManager();

			var store = new RedisTimeoutPersistence(new JsonSerializer(), clientManager);
			store.EndpointName = "timeouttest";

			clientManager.GetClient().FlushDb();

			var data = new TimeoutData()
			{
				SagaId = Guid.NewGuid(),
				Time = DateTime.UtcNow.AddSeconds(-30),
				OwningTimeoutManager = store.EndpointName,
				Destination = Address.Parse("test@access-djz9x4j"),
			};

			store.Add(data);

			DateTime nextRun;

			var timeouts = store.GetNextChunk(DateTime.UtcNow.AddMinutes(-2), out nextRun);

			Assert.IsNotEmpty(timeouts);

			TimeoutData removedData = null;

			if (store.TryRemove(timeouts.First().Item1, out removedData))
			{
				Assert.AreEqual(removedData.SagaId, data.SagaId);
			}
			else
			{
				throw new Exception("Couldn't remove");
			}

		}

		[Test]
		[ExpectedException(typeof(UniquePropertyException))]
		public void Test_Saga_Persister_Unique_Check_Works()
		{

			var clientManager = new PooledRedisClientManager();
			clientManager.GetClient().FlushDb();

			var store = new RedisSagaPersister(new JsonSerializer(), clientManager);

			var data = new TestSagaData()
			{
				Id = Guid.NewGuid(),
				CorrelationId = Guid.NewGuid(),
				Name = "Hello world"
			};

			var copy = new TestSagaData()
			{
				Id = Guid.NewGuid(),
				CorrelationId = data.CorrelationId,
				Name = "Copy"
			};

			store.Save(data);

			store.Save(copy); //Expect exception here

		}

		[Test]
		[ExpectedException(typeof(ConcurrencyException))]
		public void Test_Saga_Persister_Concurrency_Check_Works()
		{

			var clientManager = new PooledRedisClientManager();
			clientManager.GetClient().FlushDb();

			var store = new RedisSagaPersister(new JsonSerializer(), clientManager);

			var data = new TestSagaData()
			{
				Id = Guid.NewGuid(),
				CorrelationId = Guid.NewGuid(),
				Name = "Hello world"
			};

			store.Save(data);

			data.Name = data.Name + " modified";

			store.Update(data);

			var data2 = store.Get<TestSagaData>(data.Id);

			store.Update(data2);

			store.Update(data); //Causes concurrency exception


		}

		[Test]
		public void Test_Saga_Persister_Store_And_Retrieve_Works()
		{

			var clientManager = new PooledRedisClientManager();
			clientManager.GetClient().FlushDb();

			var store = new RedisSagaPersister(new JsonSerializer(), clientManager);

			var data1 = new TestSagaData()
			{
				Id = Guid.NewGuid(),
				CorrelationId = Guid.NewGuid(),
				Name = "Hello world"
			};

			store.Save(data1);

			data1.Name = data1.Name + " modified";

			store.Update(data1);

			var data2 = store.Get<TestSagaData>(data1.Id);

			Assert.AreEqual(data1.Name, data2.Name);

			var data3 = store.Get<TestSagaData>("CorrelationId", data1.CorrelationId);

			Assert.AreEqual(data3.CorrelationId, data1.CorrelationId);

			store.Complete(data1);

			Assert.IsTrue(clientManager.GetClient().SearchKeys("nservicebus:*").Count == 0);

		}

		[Test]
		public void Queue_Manager_Test()
		{
			var clientManager = new PooledRedisClientManager();
			clientManager.GetClient().FlushDb();
			var manager = new QueueManager(new JsonSerializer(), clientManager, new QueueKeyNameProvider(false));
			var messageMapper = new MessageMapper();
			messageMapper.Initialize(new[] { typeof(TestMessage), typeof(TestEvent) });

			var nsbSerializer = new JsonMessageSerializer(messageMapper);

			var message = new TestMessage()
			{
				Name = "Bob"
			};

			var transportMessage = new TransportMessage()
			{
				Id = Guid.NewGuid().ToString("N"),
				MessageIntent = MessageIntentEnum.Send,
				Headers = new Dictionary<string,string>()
			};

			transportMessage.Headers["NServiceBus.FailedQ"] = "original";

			using (var ms = new MemoryStream())
			{
				nsbSerializer.Serialize(new[] { message }, ms);
				transportMessage.Body = ms.ToArray();
			}

			var errorAddress = Address.Parse("error");

			manager.SendMessageToQueue(transportMessage, errorAddress);

			var messageCount = manager.GetMessageCount(errorAddress);

			var messages = manager.GetAllMessages(errorAddress);

			Assert.IsTrue(messages.Count == 1);
			Assert.IsTrue(messages[0].Id == transportMessage.Id);

			Assert.IsTrue(messageCount == 1);

			var queues = manager.GetAllQueues();

			Assert.IsTrue(queues.Count == 1);

			manager.ReturnAllMessagesToSourceQueue(errorAddress);

			var queues2 = manager.GetAllQueues();

			Assert.IsTrue(queues2.Count(o => o.Queue == "original") > 0);

			manager.DeleteQueue(Address.Parse("original"));

			var queues3 = manager.GetAllQueues();

			Assert.IsTrue(queues3.Count == 0);


		}

		[Test]
		public void Low_Level_Transport_Test()
		{
			
			var clientManager = new PooledRedisClientManager();

			var messageMapper = new MessageMapper();
			messageMapper.Initialize(new[] { typeof(TestMessage), typeof(TestEvent) });

			var sendAddress = Address.Parse("lowlevel@localhost");

			var queue = new RedisQueue(new JsonSerializer(), clientManager, new QueueKeyNameProvider(true));
			queue.Init(sendAddress, true);

			var nsbSerializer = new JsonMessageSerializer(messageMapper);

			var message = new TestMessage()
			{
				Name = "Bob"
			};

			var transportMessage = new TransportMessage()
			{
				MessageIntent = MessageIntentEnum.Send
			};

			using (var ms = new MemoryStream())
			{
				nsbSerializer.Serialize(new [] {message}, ms);
				transportMessage.Body = ms.ToArray();
			}

			using (var tran = new TransactionScope())
			{
				for (int x = 0; x < 2; x++)
				{
					queue.Send(transportMessage, sendAddress);
				}
				tran.Complete();
			}
			
			for (int x = 0; x < 2; x++)
			{
				if (queue.HasMessage())
				{
					using (var tran = new TransactionScope())
					{
						queue.Receive();
						tran.Complete();
					}
				}
			}
		}

		[Test]
		public void Can_Send_Message_To_Redis()
		{
			var clientManager = new PooledRedisClientManager();
			clientManager.GetClient().FlushDb();

			var bus = GetBus();

			bus.Subscribe(typeof(TestEvent));
			bus.Unsubscribe(typeof(TestEvent));
			bus.Subscribe(typeof(TestEvent));

			Thread.Sleep(1000);

			bus.Publish(new TestEvent() { Name = "Event1" });
			bus.Publish(new TestEvent() { Name = "Event2" });
			bus.Publish(new TestEvent() { Name = "Event3" });
			bus.Publish(new TestEvent() { Name = "Event4" });
			
			using (var scope = new TransactionScope(TransactionScopeOption.Required))
			{
				bus.Send("transactionqueue1", new TestMessage { Name = "Keith" });
				bus.Send("transactionqueue2", new TestMessage() { Name = "TestKeith" });
			}

			bus.Send("test", new TestMessage() { Name = "Mackie1" });
			bus.Send("test", new TestMessage() { Name = "Mackie2" });
			bus.Send("test", new TestMessage() { Name = "Mackie3" });

			bus.Publish(new TestEvent() { Name = "Event!" });

			Thread.Sleep(10000);

		}
	}

	public class TestMessage : ICommand
	{
		public string Name { get; set; }
	}

	public class TestEvent : IEvent
	{

		public string Name { get; set; }

	}

	public class TestSagaData : ISagaEntity
	{

		#region ISagaEntity Members

		public Guid Id { get; set; }

		public string OriginalMessageId { get; set; }

		public string Originator { get; set; }

		#endregion

		[Unique]
		public Guid CorrelationId { get; set; }

		public string Name { get; set; }

		public long Version { get; set; }


	}

	public class TestMessageHandler : IHandleMessages<TestMessage>
	{
		public void Handle(TestMessage message)
		{
			var log = log4net.LogManager.GetLogger(typeof(TestMessageHandler));
			log.Debug(message.Name);
			//Debug.WriteLine(message.Name);
			//throw new Exception("Forced fail!");
		}
	}

	public class TestEventHandler : IHandleMessages<TestEvent>
	{
		#region IMessageHandler<TestEvent> Members

		public void Handle(TestEvent message)
		{
			var log = log4net.LogManager.GetLogger(typeof(TestEventHandler));
			log.Debug(message.Name);
		}

		#endregion
	}
}
