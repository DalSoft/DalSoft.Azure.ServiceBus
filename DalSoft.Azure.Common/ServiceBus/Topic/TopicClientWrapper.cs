using System;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.Common.ServiceBus.Topic
{
    internal class TopicClientWrapper : IServiceBusWrapper
    {
        private readonly TopicClient _topicClient;
        private readonly SubscriptionClient _subscriptionClient;

        public TopicClientWrapper(string connectionString, string topicName)
        {
            //Since Azure SDK 2.1 we have RetryPolicy .RetryPolicy.Default equals new RetryExponential(TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(30.0), TimeSpan.FromSeconds(3.0), TimeSpan.FromSeconds(3.0), 10); which will retry exceptions that have IsTransient a maximum of 10 times http://stackoverflow.com/questions/18499661/servicebus-retryexponential-property-meanings
            _topicClient = TopicClient.CreateFromConnectionString(connectionString, topicName);
            _subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topicName, topicName);
        }

        public void OnMessageAsync(Func<BrokeredMessage, Task> onMessageCallback, OnMessageOptions onMessageOptions)
        {
            _subscriptionClient.OnMessageAsync(onMessageCallback, onMessageOptions);
        }

        public Task SendAsync(BrokeredMessage message)
        {
            return _topicClient.SendAsync(message);
        }

        public void Close()
        {
            _subscriptionClient.Close();
            _topicClient.Close();
        }
    }
}
