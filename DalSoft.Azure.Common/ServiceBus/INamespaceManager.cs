using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.Common.ServiceBus
{
    public interface INamespaceManager
    {
        bool QueueExists(string path);
        bool TopicExists(string path);
        bool SubscriptionExists(string path, string subscriptionName);
        QueueDescription CreateQueue(string path, int maxDeliveryCount);
        TopicDescription CreateTopic(string path);
        SubscriptionDescription CreateSubscription(string path, string subscriptionName);
        QueueDescription GetQueue(string path);
        TopicDescription GetTopic(string path);
        void DeleteQueue(string path);
        void DeleteTopic(string path);
        string ConnectionString { get; }
    }
}
