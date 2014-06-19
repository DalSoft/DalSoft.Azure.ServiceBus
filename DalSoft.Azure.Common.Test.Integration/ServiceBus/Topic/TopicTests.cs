using DalSoft.Azure.Common.ServiceBus.Topic;
using NUnit.Framework;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace DalSoft.Azure.Common.Test.Integration.ServiceBus.Topic
{
    /* If these intergation tests become too slow you can use one queue per test so that you can run in parallel without the locking. */
    [TestFixture]
    public class TopicTests
    {
        private static readonly object Lock = new object();
        private const int TestTimeout = 200000;
        private static readonly string ConnectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];

        [SetUp]
        public void SetUp()
        {
            Monitor.Enter(Lock); //Access the TestQueue one test at a time
        }

        [TearDown]
        public void TearDown()
        {
           new Topic<TestTopic>(ConnectionString).DeleteTopic(); //Ensure queue is delete at the end of each test
           Monitor.Exit(Lock); //Access the TestQueue one test at a time
        }

        [Test]
        public async void Subscribe_ProvidedWithMessage_MessageIsReceivedByEachubscriberAndRemovedFromQueue()
        {
            TestMessage receievedMessage = null;
            var cancellationTokenSource = new CancellationTokenSource(TestTimeout);
            var receivedCount = 0;

            var topic = new Topic<TestTopic>(ConnectionString);
            topic.Subscribe(async message => //First Subscriber
            {
                receievedMessage = message;
                receivedCount++;
                await Task.FromResult(0);
            }, cancellationTokenSource); //Give it time to process the message


            var topic2 = new Topic<TestTopic>(ConnectionString);
            topic2.Subscribe(async message => //second Subscriber
            {
                receievedMessage = message;
                receivedCount++;
                await Task.FromResult(0);
            }, cancellationTokenSource); //Give it time to process the message
            
            await topic2.Publish(new TestMessage { Id = 1, Name = "My Test" });

            await Task.Delay(15000);
            Assert.That(receivedCount, Is.EqualTo(2));
            Assert.That(receievedMessage.Id, Is.EqualTo(1));
            
        }
    }
}
