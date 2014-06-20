using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading.Tasks;

namespace DalSoft.Azure.Common.ServiceBus.Topic
{
    internal class TopicClientWrapper : IServiceBusClientWrapper
    {
        private readonly string _connectionString;
        private readonly string _topicName;
        public string SubscriptionName { get; set; }
        private readonly TopicClient _topicClient;
        private SubscriptionClient _subscriptionClient;

        
        public TopicClientWrapper(string connectionString, string topicName)
        {
            _connectionString = connectionString;
            _topicName = topicName;
            //Since Azure SDK 2.1 we have RetryPolicy .RetryPolicy.Default equals new RetryExponential(TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(30.0), TimeSpan.FromSeconds(3.0), TimeSpan.FromSeconds(3.0), 10); which will retry exceptions that have IsTransient a maximum of 10 times http://stackoverflow.com/questions/18499661/servicebus-retryexponential-property-meanings
            _topicClient = TopicClient.CreateFromConnectionString(connectionString, topicName);
        }

        public void OnMessageAsync(Func<BrokeredMessage, Task> onMessageCallback, OnMessageOptions onMessageOptions)
        {
            _subscriptionClient = SubscriptionClient.CreateFromConnectionString(_connectionString, _topicName, SubscriptionName);
            _subscriptionClient.OnMessageAsync(onMessageCallback, onMessageOptions);
        }

        public Task SendAsync(BrokeredMessage message)
        {
            return _topicClient.SendAsync(message);
        }

        public void Close()
        {
            if (_subscriptionClient!=null)
                _subscriptionClient.Close();
            
            _topicClient.Close();
        }
    }
}
