using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DalSoft.Azure.ServiceBus.Topic;
using NUnit.Framework;

namespace DalSoft.Azure.ServiceBus.Test.Integration.Topic
{
    /* If these intergation tests become too slow you can use one queue per test so that you can run in parallel without the locking. */
    [TestFixture]
    public class TopicTests
    {
        private static readonly object Lock = new object();
        private const int TestTimeout = 1500;
        private static readonly string ConnectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];

        [TearDown]
        public void TearDown()
        {
           new Topic<TestTopic>(ConnectionString, Guid.NewGuid().ToString()).DeleteTopic(); //Ensure queue is delete at the end of each test
        }

        [Test]
        public async void Subscribe_ProvidedWithMessage_MessageIsReceivedByEachSubscriberAndRemoved()
        {
            TestMessage receievedMessage = null;
            var receivedCount = 0;

            var consumer = new Topic<TestTopic>(ConnectionString, Guid.NewGuid().ToString());
            consumer.Subscribe(async message => //First Subscriber
            {
                receievedMessage = message;
                receivedCount++;
                await Task.FromResult(0);
            }, new CancellationTokenSource(TestTimeout * 2)); //Give it time to process the message

            var consumer2 = new Topic<TestTopic>(ConnectionString, Guid.NewGuid().ToString());
            consumer2.Subscribe(async message => //second Subscriber
            {
                receievedMessage = message;
                receivedCount++;
                await Task.FromResult(0);
            }, new CancellationTokenSource(TestTimeout * 2));

            using (var producer = new Topic<TestTopic>(ConnectionString))
            {
                await producer.Publish(new TestMessage { Id = 1, Name = "My Test" });
            }
            
            await Task.Delay(TestTimeout * 2); //Give it time to process the message
            
            Assert.That(receivedCount, Is.EqualTo(2));
            Assert.That(receievedMessage.Id, Is.EqualTo(1));
            Debug.WriteLine(Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetSubscription(consumer.TopicName, consumer.SubscriptionId).MessageCount);
            Debug.WriteLine(Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).GetSubscription(consumer2.TopicName, consumer2.SubscriptionId).MessageCount);

            consumer.Dispose();
            consumer2.Dispose();
        }

        [Test]
        public void Ctor_DeleteSubscriptionOnDisposeIsTrueAndSubscriptionIdIsNotNull_DeletesSubscriptionOnDispose()
        {
            var subscriptionId = Guid.NewGuid().ToString();
            string topicName;
            using (var topic = new Topic<TestTopic>(ConnectionString, subscriptionId))
            {
                topicName = topic.TopicName;
                topic.Subscribe(async message => //First Subscriber
                {
                    await Task.FromResult(0);
                }); //Give it time to process the message

                Assert.That(Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).SubscriptionExists(topicName, subscriptionId), Is.True);
            }

            Assert.That(Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ConnectionString).SubscriptionExists(topicName, subscriptionId), Is.False);
        }
    }
}
