using System.Threading.Tasks;
using DalSoft.Azure.Common.ServiceBus;
using DalSoft.Azure.Common.ServiceBus.Topic;
using Microsoft.ServiceBus.Messaging;
using Moq;
using NUnit.Framework;
using System;

namespace DalSoft.Azure.Common.Test.Unit.ServiceBus.Topic 
{
    [TestFixture]
    public class TopicTests
    {
        const string TestSubscriptionId = "TestSubscriptionId";
        private Mock<INamespaceManager> _mockNamespaceManager;
        private Mock<IServiceBusClientWrapper> _mockTopicClient;

        [SetUp]
        public void SetUp()
        {
            _mockNamespaceManager = new Mock<INamespaceManager>();
            _mockNamespaceManager.Setup(x => x.GetTopic(It.IsAny<string>())).Returns(new TopicDescription("test"));
            _mockTopicClient = new Mock<IServiceBusClientWrapper>();
        }

        [Test]
        public void Ctor_TopicDoesNotExist_TopicIsCreated()
        {
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(false);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object);

            _mockNamespaceManager.Verify(x => x.CreateTopic(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void Ctor_TopicDoesExist_QueueIsNotCreated()
        {
            _mockNamespaceManager.Setup(x => x.TopicExists(It.IsAny<string>())).Returns(true);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object);

            _mockNamespaceManager.Verify(x => x.CreateTopic(It.IsAny<string>()), Times.Never());
        }

        [Test]
        public void DeleteTopic_TheCorrectQueueIsDeleted()
        {
            var expectedQueueToBeDeleted = new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object).TopicName;

            _mockNamespaceManager.Setup(x => x.TopicExists(It.IsAny<string>())).Returns(false);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object).DeleteTopic();

            _mockNamespaceManager.Verify(x => x.DeleteTopic(expectedQueueToBeDeleted), Times.Once());
        }

        [Test]
        public void GetTopicName_TopicName_ByConventionIsReturned()
        {
            const string queueNameByConvention = "DalSoft.Azure.Common.Test.Unit.ServiceBus.TestTopic";

            Assert.That(new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object).TopicName, Is.EqualTo(queueNameByConvention));
        }

        [Test]
        public void GetTopicName_TopicNameByConventionIsGreaterThan260Characters_ThrowsArgumentException()
        {
            const string expectedMessage = "Queue name can't be > 260 characters. Make your namespace or class name shorter.";
            var exceptionResult = Assert.Throws<ArgumentException>(() => new Topic<TestQueueGreaterThan260Charactersxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object));

            Assert.That(exceptionResult.Message, Is.EqualTo(expectedMessage));
        }

        [Test]
        public void Ctor_SubscriptionId_IsSet()
        {
            Assert.That(new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object).SubscriptionId,
                Is.EqualTo(TestSubscriptionId));
        }

        [Test]
        public void Subscribe_SubscriptionIdIsNull_ThrowsArgumentNullException()
        {
            const string expectedMessage = "Please supply a subscriptionId to the constructor for your subscription";
            var exceptionResult = Assert.Throws<ArgumentNullException>(() => 
                new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, null, false, () => new Mock<IServiceBusClientWrapper>().Object).
                Subscribe(async x=>{ await Task.FromResult(0);}));

            Assert.That(exceptionResult.Message, Is.StringContaining(expectedMessage));
        }

        [Test]
        public void Subscribe_SubscriptionIdIsGreaterThan50Characters_ThrowsArgumentException()
        {
            const string expectedMessage = "subscriptionId provided to the constructor can't be > 50 characters";
            const string subscriptionIdGreaterThan50Characters = "subscriptionIdGreaterThan50Charactersxxxxxxxxxxxxxx";
            var exceptionResult = Assert.Throws<ArgumentException>(() => 
                new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, subscriptionIdGreaterThan50Characters, true, () => new Mock<IServiceBusClientWrapper>().Object)
                .Subscribe(async x => { await Task.FromResult(0); }));

            Assert.That(exceptionResult.Message, Is.StringContaining(expectedMessage));
        }

        [Test]
        public void Subscribe_SubscriptionDoesNotExist_SubscriptionIsCreated()
        {
            _mockNamespaceManager.Setup(x => x.SubscriptionExists(It.IsAny<string>(), It.IsAny<string>())).Returns(false);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object)
                .Subscribe(async x=>{ await Task.FromResult(0); });

            _mockNamespaceManager.Verify(x => x.CreateSubscription(It.IsAny<string>(), It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void Subscribe_SubscriptionDoesExist_SubscriptionIsNotCreated()
        {
            _mockNamespaceManager.Setup(x => x.SubscriptionExists(It.IsAny<string>(), It.IsAny<string>())).Returns(true);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object)
                  .Subscribe(async x => { await Task.FromResult(0); }); ;

            _mockNamespaceManager.Verify(x => x.CreateSubscription(It.IsAny<string>(), It.IsAny<string>()), Times.Never());
        }

        [Test]
        public void Ctor_DeleteSubscriptionOnDisposeIsTrueAndSubscriptionIdIsNotNull_DeletesSubscriptionOnDispose()
        {

            using (var topic = new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object))
            {
            }

            _mockNamespaceManager.Verify(x => x.DeleteSubscription(ServiceBusCommon<TestTopic>.GetName(), TestSubscriptionId), Times.Once());
        }

        [Test]
        public void Ctor_DeleteSubscriptionOnDisposeIsFalse_DoesNotDeleteSubscriptionOnDispose()
        {

            using (var topic = new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, TestSubscriptionId, false, () => new Mock<IServiceBusClientWrapper>().Object))
            {
            }

            _mockNamespaceManager.Verify(x => x.DeleteSubscription(ServiceBusCommon<TestTopic>.GetName(), TestSubscriptionId), Times.Never());
        }

        [Test]
        public void Ctor_SubscriptionIdIsNull_DoesNotDeleteSubscriptionOnDispose()
        {
            string testSubscriptionId = null;
            using (var topic = new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, testSubscriptionId, true, () => new Mock<IServiceBusClientWrapper>().Object))
            {
            }

            _mockNamespaceManager.Verify(x => x.DeleteSubscription(ServiceBusCommon<TestTopic>.GetName(), TestSubscriptionId), Times.Never());
        }
    }
}
