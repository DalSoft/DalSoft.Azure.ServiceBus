using DalSoft.Azure.Common.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Moq;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DalSoft.Azure.Common.Test.Unit.ServiceBus
{
    [TestFixture]
    public class ServiceBusCommonTests
    {
        private Mock<IServiceBusClientWrapper> _mockQueueClient;

        [SetUp]
        public void SetUp()
        {
           _mockQueueClient = new Mock<IServiceBusClientWrapper>();
        }

        [Test]
        public void OnMessage_NullOnMessagePassed_ThrowsArgumentNullException()
        {
            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            var exceptionResult = Assert.Throws<ArgumentNullException>(async () =>
                await serviceBusCommon.OnMessage(null, null, null, new CancellationTokenSource())
            );

            Assert.That(exceptionResult.ParamName, Is.EqualTo("onMessage"));
            Assert.That(exceptionResult.Message, Is.StringContaining("You must provide a Task to be invoked when the pump receives a brokeredMessage"));
        }

        [Test]
        public void OnMessage_OnMessageOptionsPassedWithAutoCompleteSetToFalse_ThrowsInvalidOperationException()
        {
            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);
            var exceptionResult = Assert.Throws<InvalidOperationException>(() => serviceBusCommon.OnMessage(async message => { await Task.FromResult(0); }, null, new OnMessageOptions { AutoComplete = false }, new CancellationTokenSource()));

            Assert.That(exceptionResult.Message, Is.EqualTo("This OnMessage cannot work with OnMessageOptions.AutoComplete set to false"));
        }

        [Test]
        public void OnMessage_NullOnMessageOptionsPassed_OnMessageOptionsSetByDefault()
        {
            _mockQueueClient.Setup(x => x.OnMessageAsync(It.IsAny<Func<BrokeredMessage, Task>>(), It.IsAny<OnMessageOptions>()))
               .Callback((Func<BrokeredMessage, Task> x, OnMessageOptions onMessageOption) =>
               {
                   Assert.That(onMessageOption, Is.Not.Null);
                   Assert.That(onMessageOption.AutoComplete, Is.EqualTo(true));

               });

            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            serviceBusCommon.OnMessage(async message => { await Task.FromResult(0); }, null, null, new CancellationTokenSource());
        }

        [Test]
        public void OnMessage_OnMessageOptionsPassed_OnMessageOptionsSet()
        {
            var expectedOnMessageOptions = new OnMessageOptions { MaxConcurrentCalls = 100 };

            _mockQueueClient.Setup(x => x.OnMessageAsync(It.IsAny<Func<BrokeredMessage, Task>>(), It.IsAny<OnMessageOptions>()))
               .Callback((Func<BrokeredMessage, Task> x, OnMessageOptions onMessageOption) =>
                   Assert.That(onMessageOption.MaxConcurrentCalls, Is.EqualTo(expectedOnMessageOptions.MaxConcurrentCalls)));

            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            serviceBusCommon.OnMessage(async message => { await Task.FromResult(0); }, null, expectedOnMessageOptions, new CancellationTokenSource());
        }

        [Test]
        public async void OnMessage_OnMessageCallback_IsInvokedCorrectlyWhenTheMessageIsProcessed()
        {
            TestMessage messageResult = null;
            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, MockPump);
            var cancellationTokenSource = new CancellationTokenSource();
            await serviceBusCommon.OnMessage(async message =>
            {
                messageResult = message;
                await Task.FromResult(0);
                cancellationTokenSource.Cancel(); //Process the message and cancel

            }, null, null, cancellationTokenSource);

            Assert.That(messageResult, Is.Not.Null);
            Assert.That(messageResult.Id, Is.EqualTo(_expectedMessage.Id));
            Assert.That(messageResult.Name, Is.EqualTo(_expectedMessage.Name));
        }

        [Test]
        public void Send_ProvidedWithANullMessaage_ThrowsArgumentNullException()
        {
            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            var exceptionResult = Assert.Throws<ArgumentNullException>(() => serviceBusCommon.Send((TestMessage)null, null));

            Assert.That(exceptionResult.ParamName, Is.EqualTo("message"));
            Assert.That(exceptionResult.Message, Is.StringContaining("Message can't be null"));
        }

        [Test]
        public async void Send_ProvidedWithAMessaage_MessageIsQueued()
        {
            _mockQueueClient.Setup(x => x.SendAsync(It.IsAny<BrokeredMessage>())).Returns(Task.Run(() => new BrokeredMessage(_expectedMessage)));
            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            await serviceBusCommon.Send(_expectedMessage, null);

            _mockQueueClient.Verify(x => x.SendAsync(It.Is<BrokeredMessage>(m => m.Clone().GetBody<TestMessage>().Id == _expectedMessage.Id)), Times.Once());
            _mockQueueClient.Verify(x => x.SendAsync(It.Is<BrokeredMessage>(m => m.Clone().GetBody<TestMessage>().Name == _expectedMessage.Name)), Times.Once());
        }

        [Test]
        public async void Send_ErrorQueueingMessageAndProvidedWithOnErrorCallback_ExceptionPassedBackCorrectlyToOnErrorCallback()
        {
            const string errorMessage = "test exception";
            AggregateException exceptionResult = null;

            _mockQueueClient.Setup(x => x.SendAsync(It.IsAny<BrokeredMessage>())).Returns(Task.Run(() => { throw new InvalidOperationException(errorMessage); }));

            var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object);

            await serviceBusCommon.Send(_expectedMessage, exception => { exceptionResult = exception; });

            Assert.That(exceptionResult, Is.Not.Null);
            Assert.That(exceptionResult.InnerException, Is.InstanceOf<InvalidOperationException>());
            Assert.That(exceptionResult.InnerException.Message, Is.EqualTo(errorMessage));

        }

        [Test]
        public async void ServiceBusCommon_OnDispose_QueueClientCallsClose()
        {
            _mockQueueClient.Setup(x => x.SendAsync(It.IsAny<BrokeredMessage>())).Returns(Task.Run(() => new BrokeredMessage(_expectedMessage)));
            using (var serviceBusCommon = new ServiceBusCommon<TestQueue>(_mockQueueClient.Object, () => new Mock<IServiceBusClientWrapper>().Object))
            {
                await serviceBusCommon.Send(_expectedMessage, null);
            }

            _mockQueueClient.Verify(x => x.Close(), Times.Once());
        }

        private readonly TestMessage _expectedMessage = new TestMessage { Id = 100, Name = "Darran Jones" };
        private IServiceBusClientWrapper MockPump()
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
