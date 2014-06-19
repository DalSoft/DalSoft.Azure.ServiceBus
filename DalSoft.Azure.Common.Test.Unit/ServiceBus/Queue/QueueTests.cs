using System;
using System.Threading;
using System.Threading.Tasks;
using DalSoft.Azure.Common.ServiceBus;
using DalSoft.Azure.Common.ServiceBus.Queue;
using Microsoft.ServiceBus.Messaging;
using Moq;
using NUnit.Framework;

namespace DalSoft.Azure.Common.Test.Unit.ServiceBus.Queue
{
    [TestFixture]
    public class QueueTests
    {
        public const int MaxDeliveryCount = 3;
        private Mock<INamespaceManager> _mockNamespaceManager;
        private Mock<IQueueClientWrapper> _mockQueueClient;

        [SetUp]
        public void SetUp()
        {
            _mockNamespaceManager = new Mock<INamespaceManager>();
            _mockNamespaceManager.Setup(x => x.GetQueue(It.IsAny<string>())).Returns(new QueueDescription("test"){ MaxDeliveryCount = MaxDeliveryCount });
            
            _mockQueueClient = new Mock<IQueueClientWrapper>();
        }

        [Test]
        public void Ctor_QueueDoesNotExist_QueueIsCreated()
        {
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(false);
            
            new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            _mockNamespaceManager.Verify(x => x.CreateQueue(It.IsAny<string>(), MaxDeliveryCount), Times.Once());
        }

        [Test]
        public void Ctor_QueueDoesExist_QueueIsNotCreated()
        {
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(true);

            new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            _mockNamespaceManager.Verify(x => x.CreateQueue(It.IsAny<string>(), MaxDeliveryCount), Times.Never());
        }

        [Test]
        public void Ctor_SettingADifferentMaxDeliveryCountForAExistingQueue_ThrowsInvalidOperationException()
        {
            const int differentMaxDeliveryCount = 6;
            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(true);
            _mockNamespaceManager.Setup(x => x.GetQueue(It.IsAny<string>())).Returns(new QueueDescription("test") { MaxDeliveryCount = differentMaxDeliveryCount });
            
            Assert.Throws<InvalidOperationException>(()=> new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount), "The Azure SDK 2.3 only lets you set the MaxDeliveryCount when first creating the Queue. For existing queues you will need to change the MaxDeliveryCount manually via the Azure portal.");
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
            var expectedQueueToBeDeleted = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount).QueueName;

            _mockNamespaceManager.Setup(x => x.QueueExists(It.IsAny<string>())).Returns(false);

            new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount).DeleteQueue();

            _mockNamespaceManager.Verify(x => x.DeleteQueue(expectedQueueToBeDeleted), Times.Once());
        }

        [Test]
        public void GetQueueName_QueueName_ByConventionIsReturned()
        {
            const string queueNameByConvention = "DalSoft.Azure.Common.Test.Unit.ServiceBus.Queue.TestQueue";

            Assert.That(new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount).QueueName, Is.EqualTo(queueNameByConvention));
        }

        [Test]
        public void GetQueueName_QueueNameByConventionIsGreaterThan260Characters_ThrowsFormatException()
        {
            const string expectedMessage = "Queue name can't be > 260 characters. Make your namespace or class name shorter.";
            var exceptionResult = Assert.Throws<FormatException>(() => new Queue<TestQueueGreaterThan260Charactersxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount));

            Assert.That(exceptionResult.Message, Is.EqualTo(expectedMessage));
        }

        [Test]
        public void Pump_NullOnMessagePassed_ThrowsArgumentNullException()
        {
            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            var exceptionResult =  Assert.Throws<ArgumentNullException>(async () => 
                await queue.Pump(null)
            );

            Assert.That(exceptionResult.ParamName, Is.EqualTo("onMessage"));
            Assert.That(exceptionResult.Message, Is.StringContaining("You must provide a Task to be invoked when the pump receives a brokeredMessage"));
        }


        [Test]
        public void Pump_OnMessageOptionsPassedWithAutoCompleteSetToFalse_ThrowsInvalidOperationException()
        {
            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);
            var exceptionResult = Assert.Throws<InvalidOperationException>(() => queue.Pump(async message => { await Task.FromResult(0); }, new OnMessageOptions { AutoComplete = false }));

            Assert.That(exceptionResult.Message, Is.EqualTo("This Pump cannot work with OnMessageOptions.AutoComplete set to false"));
        }

        [Test]
        public void Pump_NullOnMessageOptionsPassed_OnMessageOptionsSetByDefault()
        {
            _mockQueueClient.Setup(x => x.OnMessageAsync(It.IsAny<Func<BrokeredMessage, Task>>(), It.IsAny<OnMessageOptions>()))
               .Callback((Func<BrokeredMessage, Task> x, OnMessageOptions onMessageOption) =>
               {
                   Assert.That(onMessageOption, Is.Not.Null);
                   Assert.That(onMessageOption.AutoComplete, Is.EqualTo(true));
                   
               });

            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            queue.Pump(async message => { await Task.FromResult(0); });
        }

        [Test]
        public void Pump_OnMessageOptionsPassed_OnMessageOptionsSet()
        {
            var expectedOnMessageOptions = new OnMessageOptions { MaxConcurrentCalls = 100};
            
            _mockQueueClient.Setup(x => x.OnMessageAsync(It.IsAny<Func<BrokeredMessage, Task>>(), It.IsAny<OnMessageOptions>()))
               .Callback((Func<BrokeredMessage, Task> x, OnMessageOptions onMessageOption) => 
                   Assert.That(onMessageOption.MaxConcurrentCalls, Is.EqualTo(expectedOnMessageOptions.MaxConcurrentCalls)));

            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            queue.Pump(async message => { await Task.FromResult(0); }, expectedOnMessageOptions);
        }

        [Test]
        public async void Pump_OnMessageCallback_IsInvokedCorrectlyWhenTheMessageIsProcessed()
        {
            TestMessage messageResult = null;
            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, MockPump, MaxDeliveryCount);
            var cancellationTokenSource = new CancellationTokenSource();
            await queue.Pump(async message =>
            {
                messageResult = message; 
                await Task.FromResult(0);
                cancellationTokenSource.Cancel(); //Process the message and cancel
            
            }, cancellationTokenSource); 
            
            Assert.That(messageResult, Is.Not.Null);
            Assert.That(messageResult.Id, Is.EqualTo(_expectedMessage.Id));
            Assert.That(messageResult.Name, Is.EqualTo(_expectedMessage.Name));
        }

        [Test]
        public void Enqueue_ProvidedWithANullMessaage_ThrowsArgumentNullException()
        {
            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            var exceptionResult = Assert.Throws<ArgumentNullException>(() => queue.Enqueue((TestMessage)null));

            Assert.That(exceptionResult.ParamName, Is.EqualTo("message"));
            Assert.That(exceptionResult.Message, Is.StringContaining("Message can't be null"));
        }

        [Test]
        public async void Enqueue_ProvidedWithAMessaage_MessageIsQueued()
        {
            _mockQueueClient.Setup(x => x.SendAsync(It.IsAny<BrokeredMessage>())).Returns(Task.Run(() => new BrokeredMessage(_expectedMessage)));
            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            await queue.Enqueue(_expectedMessage);

            _mockQueueClient.Verify(x => x.SendAsync(It.Is<BrokeredMessage>(m => m.Clone().GetBody<TestMessage>().Id ==_expectedMessage.Id)), Times.Once());
            _mockQueueClient.Verify(x => x.SendAsync(It.Is<BrokeredMessage>(m => m.Clone().GetBody<TestMessage>().Name == _expectedMessage.Name)), Times.Once());
        }

        [Test]
        public async void Enqueue_ErrorQueueingMessageAndProvidedWithOnErrorCallback_ExceptionPassedBackCorrectlyToOnErrorCallback()
        {
            const string errorMessage = "test exception";
            AggregateException exceptionResult = null;

            _mockQueueClient.Setup(x => x.SendAsync(It.IsAny<BrokeredMessage>())).Returns(Task.Run(() => { throw new InvalidOperationException(errorMessage); }));

            var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount);

            await queue.Enqueue(_expectedMessage, exception => { exceptionResult = exception; });

            Assert.That(exceptionResult, Is.Not.Null);
            Assert.That(exceptionResult.InnerException, Is.InstanceOf<InvalidOperationException>());
            Assert.That(exceptionResult.InnerException.Message, Is.EqualTo(errorMessage));

        }

        [Test]
        public async void Enqueue_OnDispose_QueueClientCallsClose()
        {
            _mockQueueClient.Setup(x => x.SendAsync(It.IsAny<BrokeredMessage>())).Returns(Task.Run(() => new BrokeredMessage(_expectedMessage)));
            using(var queue = new Queue<TestQueue>(_mockNamespaceManager.Object, _mockQueueClient.Object, ()=>new Mock<IQueueClientWrapper>().Object, MaxDeliveryCount))
            {
                await queue.Enqueue(_expectedMessage);
            }
            
            _mockQueueClient.Verify(x=>x.Close(), Times.Once());
        }

        private readonly TestMessage _expectedMessage = new TestMessage{ Id=100, Name="Darran Jones" };
        private IQueueClientWrapper MockPump()
        {
            _mockQueueClient.Setup(x => x.OnMessageAsync(It.IsAny<Func<BrokeredMessage, Task>>(), It.IsAny<OnMessageOptions>()))
                .Callback((Func<BrokeredMessage, Task> x, OnMessageOptions onMessageOption) => x(new BrokeredMessage(_expectedMessage)
                {
                    ContentType = typeof(TestMessage).AssemblyQualifiedName
                }));

            return _mockQueueClient.Object;
        }
    }
}
