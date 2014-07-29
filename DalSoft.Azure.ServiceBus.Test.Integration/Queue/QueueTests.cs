using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using DalSoft.Azure.ServiceBus.Queue;
using Microsoft.ServiceBus.Messaging;
using NUnit.Framework;

namespace DalSoft.Azure.ServiceBus.Test.Integration.Queue
{
    /* If these intergation tests become too slow you can use one queue per test so that you can run in parallel without the locking. */
    [TestFixture]
    public class QueueTests
    {
        private static readonly object Lock = new object();
        private const int TestTimeout = 1500;
        private static readonly string ConnectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];

        [SetUp]
        public void SetUp()
        {
            Monitor.Enter(Lock); //Access the TestQueue one test at a time
        }

        [TearDown]
        public void TearDown()
        {
           new Queue<TestQueue>(ConnectionString).DeleteQueue(); //Ensure queue is delete at the end of each test
           Monitor.Exit(Lock); //Access the TestQueue one test at a time
        }

        [Test]
        public async void Pump_ProvidedWithMessage_MessageIsReceivedAndRemovedFromQueue()
        {
            TestMessage receievedMessage = null;
            using (var queue = new Queue<TestQueue>(ConnectionString))
            {

                await queue.Enqueue(new TestMessage { Id = 1, Name = "My Test" });
                await queue.Pump(async message =>
                {
                    receievedMessage = message;
                    await Task.FromResult(0);
                }, new CancellationTokenSource(TestTimeout)); //Give it time to process the message
                
                var queueDescription = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetQueue(queue.QueueName);
                Assert.That(queueDescription.MessageCount, Is.EqualTo(0));
                Assert.That(receievedMessage.Id, Is.EqualTo(1));
            }
            
        }

        [Test(Description = "Experimental")]
        public void NonAsyncPump_ProvidedWithMessage_MessageIsReceivedAndRemovedFromQueue()
        {
            TestMessage receievedMessage = null;
            using (var queue = new Queue<TestQueue>(ConnectionString))
            {
                queue.Pump(async message =>
                {
                    receievedMessage = message;
                    await Task.FromResult(0);
                });
                queue.Enqueue(new TestMessage { Id = 1, Name = "My Test" });
                Thread.Sleep(TestTimeout * 2); //Give it time to process the message

                var queueDescription = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetQueue(queue.QueueName);
                Assert.That(queueDescription.MessageCount, Is.EqualTo(0));
                Assert.That(receievedMessage.Id, Is.EqualTo(1));
            }   
        }

        [Test]
        public async void Pump_ProvidedWithMessageEnqueuedByBrokeredMessageOverload_MessageIsReceivedAndRemovedFromQueue()
        { 
            TestMessage receievedMessage = null;
            using (var queue = new Queue<TestQueue>(ConnectionString))
            {
                await queue.Enqueue<TestMessage>(new BrokeredMessage(new TestMessage { Id = 1, Name = "My Test" }));
                await queue.Pump(async message =>
                {
                    receievedMessage = message;
                    await Task.FromResult(0);
                }, new CancellationTokenSource(TestTimeout)); //Give it time to process the message
                
                var queueDescription = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetQueue(queue.QueueName);
                Assert.That(queueDescription.MessageCount, Is.EqualTo(0));
                Assert.That(receievedMessage.Id, Is.EqualTo(1));
            }
        }

        [Test]
        public async void Pump_OnMessageCallBackErrors_MessageIsNotRemovedFromQueue()
        {
            using (var queue = new Queue<TestQueue>(ConnectionString))
            {    
                await queue.Enqueue(new TestMessage { Id = 1, Name = "My Test" });
                await queue.Pump(message => { throw new Exception(); }, new CancellationTokenSource(TestTimeout)); //Give it time to process the message
                    
                var queueDescription = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetQueue(queue.QueueName);
                Assert.That(queueDescription.MessageCount, Is.EqualTo(1));
            }
        }

        [Test]
        public async void Pump_PumpIsCancelled_MessageIsNotRemovedFromQueue()
        {
            using (var queue = new Queue<TestQueue>(ConnectionString))
            {
                await queue.Pump(async message => { await Task.FromResult(0); }, exception => { throw exception; }, new CancellationTokenSource(TestTimeout));
                await queue.Enqueue(new TestMessage { Id = 1, Name = "My Test" });

                var queueDescription = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetQueue(queue.QueueName);
                Assert.That(queueDescription.MessageCount, Is.EqualTo(1));
            }
        }

        [Test]
        public async void Pump_OnMessageErrorAndProvidedWithOnErrorCallback_OnMessageExceptionPassedBackCorrectlyToOnErrorCallback()
        {
            const string errorMessage = "test error";
            OnMessageException onMessageException = null;

            using (var queue = new Queue<TestQueue>(ConnectionString))
            {
                await queue.Enqueue(new TestMessage { Id = 1, Name = "My Test" });
                
                await queue.Pump(message =>
                {
                    throw new ArithmeticException(errorMessage);
                }, ex =>
                {
                    onMessageException = ex as OnMessageException;
                }, new CancellationTokenSource(TestTimeout)); //Give it time to process the message
            }
               
            Assert.That(onMessageException, Is.Not.Null);
            Assert.That(onMessageException.InnerException, Is.InstanceOf<ArithmeticException>());
            Assert.That(onMessageException.Message, Is.EqualTo(errorMessage));
            Assert.That(onMessageException.BrokeredMessage.GetBody<TestMessage>().Id, Is.EqualTo(1));
        }
        
        [Test]
        public async void Pump_OnMessageUnexpectedQueueErrorAndProvidedWithOnErrorCallback_FailExceptionPassedBackCorrectlyToOnErrorCallback()
        {
            const string errorMessage = "40400: Endpoint not found.";
            MessagingEntityNotFoundException exceptionResult = null;

            using (var queue = new Queue<TestQueue>(ConnectionString))
            {
                queue.DeleteQueue();
                await queue.Pump(async message => { await Task.FromResult(0);  }, ex => { exceptionResult = ex as MessagingEntityNotFoundException; });
            }
                
            Assert.That(exceptionResult, Is.Not.Null);
            Assert.That(exceptionResult, Is.InstanceOf<MessagingEntityNotFoundException>());
            Assert.That(exceptionResult.Message, Is.StringContaining(errorMessage));   
        }

        [Test]
        public async void Pump_MessageContentTypeIsNullAndProvidedWithOnErrorCallback_InvalidOperationIsPassedBackToOnErrorCallback()
        {
            const string expectedErrorMessgeForNoContentType = "ContentType must be set to the Type of the brokeredMessage. Please send the brokeredMessage using DalSoft.Azure.ServiceBus.Queue";
            OnMessageException onMessageException = null;

            using (var queue = new Queue<TestQueue>(ConnectionString))
            {
                //Publish message without the ContentType set to the type of the message
                QueueClient.CreateFromConnectionString(ConnectionString, queue.QueueName).Send(new BrokeredMessage(new TestMessage { Id = 1, Name = "My Test" })
                {
                    ContentType = null
                });

                await queue.Pump(async message =>
                {
                    await Task.FromResult(0);
                }, ex =>
                {
                    onMessageException = ex as OnMessageException;
                }, new CancellationTokenSource(TestTimeout)); //Give it time to process the message
            }

            Assert.That(onMessageException, Is.Not.Null);
            Assert.That(onMessageException.InnerException, Is.InstanceOf<InvalidOperationException>());
            Assert.That(onMessageException.Message, Is.EqualTo(expectedErrorMessgeForNoContentType));
        }

        [Test]
        public async void Ctor_SettingMaxDeliveryCount_MessageIsTriedForTheMaxDeliveryCountAndThenDeadLettered()
        {
            using (var queue = new Queue<TestQueue>(ConnectionString, 3))
            {
                var messageTries = 0;

                await queue.Enqueue(new TestMessage { Id = 1, Name = "My Test" });

                await queue.Pump(async message =>
                {
                    await Task.FromResult(0);
                    messageTries++;
                    throw new Exception();
                }, new CancellationTokenSource(TestTimeout)); //Give it time to process the message
                    
                var queueDescription = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetQueue(queue.QueueName);
                Assert.That(messageTries, Is.EqualTo(3));  
                Assert.That(queueDescription.MessageCountDetails.DeadLetterMessageCount, Is.EqualTo(1));  
             }
        }
    }
}
