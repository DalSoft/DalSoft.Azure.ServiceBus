using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.ServiceBus.Topic
{   // ReSharper disable once UnusedTypeParameter
    public interface ITopic<TTopic> : IDisposable
    {
        void DeleteTopic();
        string TopicName { get; }
        Task Publish<TMessage>(TMessage message) where TMessage : class, new();
        Task Publish<TMessage>(TMessage message, Action<AggregateException> onError) where TMessage : class, new();
        Task Publish<TMessage>(BrokeredMessage message, Action<AggregateException> onError) where TMessage : class, new();

        Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError, OnMessageOptions onMessageOptions, CancellationTokenSource cancellationTokenSource);
        Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError, CancellationTokenSource cancellationTokenSource);
        Task Subscribe(Func<dynamic, Task> onMessage, CancellationTokenSource cancellationTokenSource);
        Task Subscribe(Func<dynamic, Task> onMessage, OnMessageOptions onMessageOptions);
        Task Subscribe(Func<dynamic, Task> onMessage, Action<Exception> onError);
        Task Subscribe(Func<dynamic, Task> onMessage);
    }
}
