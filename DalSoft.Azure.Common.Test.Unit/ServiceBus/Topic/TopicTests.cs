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

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            _mockNamespaceManager.Verify(x => x.CreateTopic(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void Ctor_TopicDoesExist_QueueIsNotCreated()
        {
            _mockNamespaceManager.Setup(x => x.TopicExists(It.IsAny<string>())).Returns(true);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            _mockNamespaceManager.Verify(x => x.CreateTopic(It.IsAny<string>()), Times.Never());
        }

        [Test]
        public void Ctor_SubscriptionDoesNotExist_SubscriptionIsCreated()
        {
            _mockNamespaceManager.Setup(x => x.SubscriptionExists(It.IsAny<string>())).Returns(false);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            _mockNamespaceManager.Verify(x => x.CreateSubscription(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void Ctor_SubscriptionDoesExist_SubscriptionIsNotCreated()
        {
            _mockNamespaceManager.Setup(x => x.SubscriptionExists(It.IsAny<string>())).Returns(true);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            _mockNamespaceManager.Verify(x => x.CreateSubscription(It.IsAny<string>()), Times.Never());
        }

        [Test]
        public void DeleteTopic_TheCorrectQueueIsDeleted()
        {
            var expectedQueueToBeDeleted = new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object).TopicName;

            _mockNamespaceManager.Setup(x => x.TopicExists(It.IsAny<string>())).Returns(false);

            new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object).DeleteTopic();

            _mockNamespaceManager.Verify(x => x.DeleteTopic(expectedQueueToBeDeleted), Times.Once());
        }

        [Test]
        public void GetTopicName_TopicName_ByConventionIsReturned()
        {
            const string queueNameByConvention = "DalSoft.Azure.Common.Test.Unit.ServiceBus.TestTopic";

            Assert.That(new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object).TopicName, Is.EqualTo(queueNameByConvention));
        }

        [Test]
        public void GetTopicName_TopicNameByConventionIsGreaterThan260Characters_ThrowsArgumentException()
        {
            const string expectedMessage = "Queue name can't be > 260 characters. Make your namespace or class name shorter.";
            var exceptionResult = Assert.Throws<ArgumentException>(() => new Topic<TestQueueGreaterThan260Charactersxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object));

            Assert.That(exceptionResult.Message, Is.EqualTo(expectedMessage));
        }

        [Test]
        public void GetSubscriptionName_SubscriptionName_ByConventionIsReturned()
        {
            const string queueNameByConvention = "TestTopic";

            Assert.That(new Topic<TestTopic>(_mockNamespaceManager.Object, _mockTopicClient.Object, () => new Mock<IServiceBusClientWrapper>().Object).SubscriptionName, Is.EqualTo(queueNameByConvention));
        }

        [Test]
        public void GetSubscriptionName_SubscriptionNameByConventionIsGreaterThan50Characters_ThrowsArgumentException()
        {
            const string expectedMessage = "Subscription name can't be > 50 characters. Make your class name shorter.";
            var exceptionResult = Assert.Throws<ArgumentException>(() => new Topic<TestTopicGreaterThan50Charactersxxxxxxxxxxxxxxxxxxx>(_mockNamespaceManager.Object, _mockTopicClient.Object, ()=>new Mock<IServiceBusClientWrapper>().Object));

            Assert.That(exceptionResult.Message, Is.EqualTo(expectedMessage));
        }
    }
}
