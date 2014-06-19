using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DalSoft.Azure.Common.ServiceBus.Topic
{
    public sealed class Topic<TTopic> : ITopic<TTopic>
    {
        private readonly INamespaceManager _namespaceManager;
        private readonly ServiceBusCommon<TTopic> _serviceBusCommon;
        public string TopicName { get { return ServiceBusCommon<TTopic>.GetName(); } }

        public Topic(string connectionString) : this(new NamespaceManager(connectionString), new TopicClientWrapper(connectionString, ServiceBusCommon<TTopic>.GetName()), () => CreateSubscriberClient(connectionString)) { }

        internal Topic(INamespaceManager namespaceManager, IServiceBusWrapper serviceBus, Func<IServiceBusWrapper> subscriberClientClient) //Unit test seam 
        {
            _namespaceManager = namespaceManager;

            if (!_namespaceManager.TopicExists(ServiceBusCommon<TTopic>.GetName()))
                _namespaceManager.CreateTopic(ServiceBusCommon<TTopic>.GetName());

            if (!_namespaceManager.SubscriptionExists(ServiceBusCommon<TTopic>.GetName()))
                _namespaceManager.CreateSubscription(ServiceBusCommon<TTopic>.GetName());

            _serviceBusCommon = new ServiceBusCommon<TTopic>(serviceBus, subscriberClientClient);
        }

        public void DeleteTopic()
        {
            _namespaceManager.DeleteQueue(ServiceBusCommon<TTopic>.GetName());
        }

        public Task Subscribe(Func<dynamic, Task> onMessage)
        {
            return Subscribe(onMessage, null, null, new CancellationTokenSource());
        }

        public Task Subscribe(Func<dynamic, Task> onMessage, OnMessageOptions onMessageOptions)
        {
            return Subscribe(onMessage, null, onMessageOptions, new CancellationTokenSource());
        }

        public Task Subscribe(Func<dynamic, Task> onMessage, CancellationTokenSource cancellationTokenSource)
        {
            return Subscribe(onMessage, null, null, cancellationTokenSource);
        }

        public Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError)
        {
            return Subscribe(onMessage, onError, null, new CancellationTokenSource());
        }

        public Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError, CancellationTokenSource cancellationTokenSource)
        {
            return Subscribe(onMessage, onError, null, cancellationTokenSource);
        }

        // TODO we could use TMessage overload to support types, the pump would check the type and if it doesn't match TMessage it will Abandon for a competing pump to pick up
        /// <summary>
        /// Initiates the Message pump and the onMessage onError is invoked for each brokeredmessage that is received, disposing will close the client and will stop the pump.
        /// </summary>
        /// <param name="onMessage">Callback when the pump receives a brokeredMessage. *Do not swallow exceptions in your onMessage onError* as the pump will not be notified of the exception and the brokeredMessage will be marked as completed!</param>
        /// <param name="onError">Callback when the pump errors unexpectedly or an exception is thrown from your onMessage onError when a receiving a brokeredMessage. This is where you would log exceptions. It is optional if not provided exceptions will be swallowed but the brokeredMessage won't be marked as complete! *Only throw/re-throw exceptions* in this onError if you want the stop the pump receiving new messages (if your in any doubt log but swallow exceptions in this onError).</param>
        /// <param name="onMessageOptions">See MSDN documentation for onMessageOptions</param>
        /// <param name="cancellationTokenSource">Used to manually stop the pump. Alternatively just dispose and the pump is cancelled and cleaned up for you.</param>
        /// <exception cref="OnMessageException">Exception type that will be passed the the onError calling  receiving a brokeredMessage from your onMessage Callback</exception>
        /// <remarks>OnMessage may only be called once per instance.</remarks>
        public Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError, OnMessageOptions onMessageOptions, CancellationTokenSource cancellationTokenSource)
        {
            return _serviceBusCommon.OnMessage(onMessage, onError, onMessageOptions, cancellationTokenSource);
        }

        public Task Publish<TMessage>(TMessage message) where TMessage : class, new() 
        {
            return Publish(message, null);
        }

        /// <summary>
        /// Adds a brokeredMessage to the queue.
        /// </summary>
        /// <param name="message">The brokeredMessage you want to add.</param>
        /// <param name="onError">Callback when adding the brokeredMessage to the queue errors unexpectedly. This is an optional onError if a onError isn't provided the exception will be thrown</param>
        /// <returns></returns>
        public Task Publish<TMessage>(TMessage message, Action<AggregateException> onError) where TMessage : class, new()
        {
            return _serviceBusCommon.Send(message, onError);
        }

        public Task Publish<TMessage>(BrokeredMessage brokeredMessage) where TMessage : class, new()
        {
            return Publish<TMessage>(brokeredMessage, null);
        }

        /// <summary>
        /// Adds a brokeredMessage to the queue.
        /// </summary>
        /// <typeparam name="TMessage">The messge</typeparam>
        /// <param name="brokeredMessage">The brokeredMessage you want to add.</param>
        /// <param name="onError">Callback when adding the brokeredMessage to the queue errors unexpectedly. This is an optional onError if a onError isn't provided the exception will be thrown</param>
        /// <returns></returns>
        public Task Publish<TMessage>(BrokeredMessage brokeredMessage, Action<AggregateException> onError) where TMessage : class, new()
        {
           return _serviceBusCommon.Send<TMessage>(brokeredMessage, onError);
        }

        private static IServiceBusWrapper CreateSubscriberClient(string connectionString)
        {   //Used so we ensure we manage the lifecycle of the pump and are able to close it
            return new TopicClientWrapper(connectionString, ServiceBusCommon<TTopic>.GetName());
        }

        public void Dispose()
        {
            _serviceBusCommon.Dispose();       
        }
    }
}


