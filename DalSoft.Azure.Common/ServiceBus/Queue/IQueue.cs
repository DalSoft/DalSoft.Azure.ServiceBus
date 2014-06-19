using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.Common.ServiceBus.Queue
{   // ReSharper disable once UnusedTypeParameter
    public interface IQueue<TQueue> : IDisposable
    {
        void DeleteQueue();
        string QueueName { get; }
        Task Enqueue<TMessage>(TMessage message) where TMessage : class, new();
        Task Enqueue<TMessage>(TMessage message, Action<AggregateException> onError) where TMessage : class, new();
        Task Enqueue<TMessage>(BrokeredMessage message, Action<AggregateException> onError) where TMessage : class, new();

        Task Pump(Func<dynamic, Task> onMessage, Action<Exception> onError, OnMessageOptions onMessageOptions, CancellationTokenSource cancellationTokenSource);
        Task Pump(Func<dynamic, Task> onMessage, Action<Exception> onError, CancellationTokenSource cancellationTokenSource);
        Task Pump(Func<dynamic, Task> onMessage, CancellationTokenSource cancellationTokenSource);
        Task Pump(Func<dynamic, Task> onMessage, OnMessageOptions onMessageOptions);
        Task Pump(Func<dynamic, Task> onMessage, Action<Exception> onError);
        Task Pump(Func<dynamic, Task> onMessage);
    }
}
