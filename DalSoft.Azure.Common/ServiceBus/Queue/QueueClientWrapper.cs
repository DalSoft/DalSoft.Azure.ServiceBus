using System;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.Common.ServiceBus.Queue
{
    internal class QueueClientWrapper : IServiceBusClientWrapper
    {
        private readonly QueueClient _queueClient;

        public QueueClientWrapper(string connectionString, string queueName)
        {
            _queueClient = QueueClient.CreateFromConnectionString(connectionString, queueName);
            //Since Azure SDK 2.1 we have RetryPolicy .RetryPolicy.Default equals new RetryExponential(TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(30.0), TimeSpan.FromSeconds(3.0), TimeSpan.FromSeconds(3.0), 10); which will retry exceptions that have IsTransient a maximum of 10 times http://stackoverflow.com/questions/18499661/servicebus-retryexponential-property-meanings
        }

        public void OnMessageAsync(Func<BrokeredMessage, Task> onMessageCallback, OnMessageOptions onMessageOptions)
        {
            _queueClient.OnMessageAsync(onMessageCallback, onMessageOptions);
        }

        public Task SendAsync(BrokeredMessage message)
        {
            return _queueClient.SendAsync(message);
        }

        public void Close()
        {
            _queueClient.Close();
        }
    }
}
