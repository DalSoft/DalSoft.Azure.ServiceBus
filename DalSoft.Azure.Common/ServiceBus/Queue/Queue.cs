using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DalSoft.Azure.Common.ServiceBus.Queue
{
    public sealed class Queue<TQueue> : IQueue<TQueue>
    {
        private readonly INamespaceManager _namespaceManager;
        private readonly ServiceBusCommon<TQueue> _serviceBusCommon;
        public string QueueName { get { return ServiceBusCommon<TQueue>.GetName(); } }

        public Queue(string connectionString) : this(new NamespaceManager(connectionString), new QueueClientWrapper(connectionString, ServiceBusCommon<TQueue>.GetName()), () => CreatePumpClient(connectionString), null) { }

        public Queue(string connectionString, int maxDeliveryCount) : this(new NamespaceManager(connectionString), new QueueClientWrapper(connectionString, ServiceBusCommon<TQueue>.GetName()), () => CreatePumpClient(connectionString), maxDeliveryCount) { }

        internal Queue(INamespaceManager namespaceManager, IServiceBusClientWrapper serviceBusClient, Func<IServiceBusClientWrapper> queuePumpClient, int? maxDeliveryCount) //Unit test seam 
        {
            _namespaceManager = namespaceManager;

            if (!_namespaceManager.QueueExists(ServiceBusCommon<TQueue>.GetName()))
            {
                _namespaceManager.CreateQueue(ServiceBusCommon<TQueue>.GetName(), maxDeliveryCount ?? 10); //10 is the Azure default
            }
            else
            {
                if (maxDeliveryCount.HasValue && _namespaceManager.GetQueue(ServiceBusCommon<TQueue>.GetName()).MaxDeliveryCount != maxDeliveryCount.Value)
                    throw new InvalidOperationException("The Azure SDK 2.3 only lets you set the MaxDeliveryCount when first creating the Queue. For existing queues you will need to change the MaxDeliveryCount manually via the Azure portal.");
            }

            _serviceBusCommon = new ServiceBusCommon<TQueue>(serviceBusClient, queuePumpClient);
        }

        public void DeleteQueue()
        {
            _namespaceManager.DeleteQueue(ServiceBusCommon<TQueue>.GetName());
        }

        public Task Pump(Func<dynamic, Task> onMessage)
        {
            return Pump(onMessage, null, null, new CancellationTokenSource());
        }

        public Task Pump(Func<dynamic, Task> onMessage, OnMessageOptions onMessageOptions)
        {
            return Pump(onMessage, null, onMessageOptions, new CancellationTokenSource());
        }

        public Task Pump(Func<dynamic, Task> onMessage, CancellationTokenSource cancellationTokenSource)
        {
            return Pump(onMessage, null, null, cancellationTokenSource);
        }

        public Task Pump(Func<dynamic, Task> onMessage, Action<Exception> onError)
        {
            return Pump(onMessage, onError, null, new CancellationTokenSource());
        }

        public Task Pump(Func<dynamic, Task> onMessage, Action<Exception> onError, CancellationTokenSource cancellationTokenSource)
        {
            return Pump(onMessage, onError, null, cancellationTokenSource);
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
        public Task Pump(Func<dynamic, Task> onMessage, Action<Exception> onError, OnMessageOptions onMessageOptions, CancellationTokenSource cancellationTokenSource)
        {
            return _serviceBusCommon.OnMessage(onMessage, onError, onMessageOptions, cancellationTokenSource);
        }

        public Task Enqueue<TMessage>(TMessage message) where TMessage : class, new() 
        {
            return Enqueue(message, null);
        }

        /// <summary>
        /// Adds a brokeredMessage to the queue.
        /// </summary>
        /// <param name="message">The brokeredMessage you want to add.</param>
        /// <param name="onError">Callback when adding the brokeredMessage to the queue errors unexpectedly. This is an optional onError if a onError isn't provided the exception will be thrown</param>
        /// <returns></returns>
        public Task Enqueue<TMessage>(TMessage message, Action<AggregateException> onError) where TMessage : class, new()
        {
            return _serviceBusCommon.Send(message, onError);
        }

        public Task Enqueue<TMessage>(BrokeredMessage brokeredMessage) where TMessage : class, new()
        {
            return Enqueue<TMessage>(brokeredMessage, null);
        }

        /// <summary>
        /// Adds a brokeredMessage to the queue.
        /// </summary>
        /// <typeparam name="TMessage">The messge</typeparam>
        /// <param name="brokeredMessage">The brokeredMessage you want to add.</param>
        /// <param name="onError">Callback when adding the brokeredMessage to the queue errors unexpectedly. This is an optional onError if a onError isn't provided the exception will be thrown</param>
        /// <returns></returns>
        public Task Enqueue<TMessage>(BrokeredMessage brokeredMessage, Action<AggregateException> onError) where TMessage : class, new()
        {
           return _serviceBusCommon.Send<TMessage>(brokeredMessage, onError);
        }

        private static IServiceBusClientWrapper CreatePumpClient(string connectionString)
        {   //Used so we ensure we manage the lifecycle of the pump and are able to close it
            return new QueueClientWrapper(connectionString, ServiceBusCommon<TQueue>.GetName());
        }

        public void Dispose()
        {
            _serviceBusCommon.Dispose();       
        }
    }
}


