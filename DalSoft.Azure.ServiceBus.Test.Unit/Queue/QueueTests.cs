using System;
using DalSoft.Azure.ServiceBus.Queue;
using Microsoft.ServiceBus.Messaging;
using Moq;
using NUnit.Framework;

namespace DalSoft.Azure.ServiceBus.Test.Unit.Queue
{
    [TestFixture]
    public class QueueTests
    {
        public const int MaxDeliveryCount = 3;
        private Mock<INamespaceManager> _mockNamespaceManager;
        private Mock<ServiceBus.IServiceBusClientWrapper> _mockQueueClient;

        [SetUp]
        public void SetUp()
        {
            _mockNamespaceManager = new Mock<INamespaceManager>();
            _mockNamespaceManager.Setup(x => x.GetQueue(It.IsAny<string>())).Returns(new QueueDescription("test"){ MaxDeliveryCount = MaxDeliveryCount });
            
            _mockQueueClient = new Mock<ServiceBus.IServiceBusClientWrapper>();
        }

        [Test]
        public void Ctor_QueueDoesNotExist_QueueIsCreated()
        {
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(false);
            
            new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount);

            _mockNamespaceManager.Verify(x => x.CreateQueue(It.IsAny<string>(), MaxDeliveryCount), Times.Once());
        }

        [Test]
        public void Ctor_QueueDoesExist_QueueIsNotCreated()
        {
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(true);

            new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount);

            _mockNamespaceManager.Verify(x => x.CreateQueue(It.IsAny<string>(), MaxDeliveryCount), Times.Never());
        }

        [Test]
        public void Ctor_SettingADifferentMaxDeliveryCountForAExistingQueue_ThrowsInvalidOperationException()
        {
            const int differentMaxDeliveryCount = 6;
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(true);
            _mockNamespaceManager.Setup(x => x.GetQueue(It.IsAny<string>())).Returns(new QueueDescription("test") { MaxDeliveryCount = differentMaxDeliveryCount });
            
            Assert.Throws<InvalidOperationException>(()=> new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount), "The Azure SDK 2.3 only lets you set the MaxDeliveryCount when first creating the Queue. For existing queues you will need to change the MaxDeliveryCount manually via the Azure portal.");
        }

        [Test]
        public void Ctor_SettingTheSameMaxDeliveryCountForAnExistingQueue_DoesNotThrow()
        {
            const int sameMaxDeliveryCount =  MaxDeliveryCount;
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(true);
            _mockNamespaceManager.Setup(x => x.GetQueue(It.IsAny<string>())).Returns(new QueueDescription("test") { MaxDeliveryCount = sameMaxDeliveryCount });

            Assert.Pass(); //Ctor didn't throw
        }

        [Test]
        public void DeleteQueue_TheCorrectQueueIsDeleted()
        {
            var expectedQueueToBeDeleted = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount).QueueName;

            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(false);

            new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount).DeleteQueue();

            _mockNamespaceManager.Verify(x => x.DeleteQueue(expectedQueueToBeDeleted), Times.Once());
        }

        [Test]
        public void GetQueueName_QueueName_ByConventionIsReturned()
        {
            const string queueNameByConvention = "DalSoft.Azure.ServiceBus.Test.Unit.TestQueue";

            Assert.That(new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount).QueueName, Is.EqualTo(queueNameByConvention));
        }

        [Test]
        public void GetQueueName_QueueNameByConventionIsGreaterThan260Characters_ThrowsArgumentException()
        {
            const string expectedMessage = "Queue name can't be > 260 characters. Make your namespace or class name shorter.";
            var exceptionResult = Assert.Throws<ArgumentException>(() => new Queue<TestQueueGreaterThan260Charactersxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<ServiceBus.IServiceBusClientWrapper>().Object, MaxDeliveryCount));

            Assert.That(exceptionResult.Message, Is.EqualTo(expectedMessage));
        }
    }
}
