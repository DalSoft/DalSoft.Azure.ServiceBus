using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.ServiceBus.Topic
{
    public sealed class Topic<TTopic> : ITopic<TTopic>
    {
        private readonly INamespaceManager _namespaceManager;
        private readonly bool _deleteSubscriptionOnDispose;
        private readonly ServiceBusCommon<TTopic> _serviceBusCommon;
        public string TopicName { get { return ServiceBusCommon<TTopic>.GetName(); } }
        public string SubscriptionId { get; private set; }

        /// <remarks>Subscription is created on demand using SubscriptionId on the first call to Subscribe()</remarks>

        public Topic(string connectionString)
            : this(new NamespaceManager(connectionString),
            new TopicClientWrapper(connectionString, ServiceBusCommon<TTopic>.GetName()), null, false, null) { }

        public Topic(string connectionString, string subscriptionId)
            : this(new NamespaceManager(connectionString),
            new TopicClientWrapper(connectionString, ServiceBusCommon<TTopic>.GetName()), subscriptionId, true, () => CreateSubscriberClient(connectionString, subscriptionId)) { }

        public Topic(string connectionString, string subscriptionId, bool deleteSubscriptionOnDispose)
            : this(new NamespaceManager(connectionString),
            new TopicClientWrapper(connectionString, ServiceBusCommon<TTopic>.GetName()), subscriptionId, deleteSubscriptionOnDispose, () => CreateSubscriberClient(connectionString, subscriptionId)) { }

        internal Topic(INamespaceManager namespaceManager, IServiceBusClientWrapper serviceBusClient, string subscriptionId, bool deleteSubscriptionOnDispose, Func<IServiceBusClientWrapper> subscriberClient) //Unit test seam 
        {
            SubscriptionId = subscriptionId;
            
            _namespaceManager = namespaceManager;
            _deleteSubscriptionOnDispose = deleteSubscriptionOnDispose;

            if (!_namespaceManager.TopicExists(ServiceBusCommon<TTopic>.GetName()))
                _namespaceManager.CreateTopic(ServiceBusCommon<TTopic>.GetName());

            _serviceBusCommon = new ServiceBusCommon<TTopic>(serviceBusClient, subscriberClient);
        }

        public void DeleteTopic()
        {
            _namespaceManager.DeleteTopic(ServiceBusCommon<TTopic>.GetName());
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
        /// <remarks>OnMessage may only be called once per instance. Subscription is created on demand using SubscriptionId on the first call to Subscribe().</remarks>
        public Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError, OnMessageOptions onMessageOptions, CancellationTokenSource cancellationTokenSource)
        {
            // ReSharper disable NotResolvedInText
            if (string.IsNullOrWhiteSpace(SubscriptionId)) throw new ArgumentNullException("subscriptionId", "Please supply a subscriptionId to the constructor for your subscription");

            if (SubscriptionId.Length > 50) throw new ArgumentException("subscriptionId provided to the constructor can't be > 50 characters", "subscriptionId");
            
            if (!_namespaceManager.SubscriptionExists(ServiceBusCommon<TTopic>.GetName(), SubscriptionId)) //TODO max delivery count is on subscribers
                _namespaceManager.CreateSubscription(ServiceBusCommon<TTopic>.GetName(), SubscriptionId);
            
            return _serviceBusCommon.OnMessage(onMessage, onError, onMessageOptions, cancellationTokenSource);
            // ReSharper restore NotResolvedInText
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

        private static IServiceBusClientWrapper CreateSubscriberClient(string connectionString, string subscriptionId)
        {   //Used so we ensure we manage the lifecycle of the pump and are able to close it
            return new TopicClientWrapper(connectionString, ServiceBusCommon<TTopic>.GetName())
            {
                SubscriptionName = subscriptionId
            };
        }

        public void Dispose()
        {
            if (_deleteSubscriptionOnDispose && SubscriptionId!=null)
                _namespaceManager.DeleteSubscription(ServiceBusCommon<TTopic>.GetName(), SubscriptionId);

            _serviceBusCommon.Dispose();       
        }
    }
}


