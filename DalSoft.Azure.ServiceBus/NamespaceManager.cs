using System.CodeDom;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.ServiceBus
{
    public sealed class NamespaceManager : INamespaceManager
    {
        private readonly Microsoft.ServiceBus.NamespaceManager _namespaceManager;

        public NamespaceManager(string connectionString)
        {
            ConnectionString = connectionString;
            _namespaceManager = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(connectionString);
        }

        public string ConnectionString
        {
            get;
            private set;
        }

        public QueueDescription GetQueue(string path)
        {
            return _namespaceManager.GetQueue(path);
        }

        public TopicDescription GetTopic(string path)
        {
            return _namespaceManager.GetTopic(path);
        }

        public bool QueueExists(string path) 
        {
            return _namespaceManager.QueueExists(path);
        }

        public bool TopicExists(string path)
        {
            return _namespaceManager.TopicExists(path);
        }

        public bool SubscriptionExists(string path, string subscriptionName)
        {
            return _namespaceManager.SubscriptionExists(path, subscriptionName);
        }

        public QueueDescription CreateQueue(string path, Settings settings)
        {   
            var queueDescription = new QueueDescription(path)
            {
                RequiresDuplicateDetection = settings.RequireDuplicateDetection
            };

            queueDescription.MaxDeliveryCount = settings.MaxDeliveryCount;

            if (settings.DuplicateDetectionHistoryTimeWindow.HasValue)
                queueDescription.DuplicateDetectionHistoryTimeWindow = settings.DuplicateDetectionHistoryTimeWindow.Value;

            return _namespaceManager.CreateQueue(queueDescription);
        }

        public TopicDescription CreateTopic(string path)
        {
            return _namespaceManager.CreateTopic(new TopicDescription(path));
        }

        public SubscriptionDescription CreateSubscription(string path, string subscriptionName)
        {
            return _namespaceManager.CreateSubscription(path, subscriptionName);
        }

        public void DeleteQueue(string path)
        {
            _namespaceManager.DeleteQueue(path);
        }

        public void DeleteTopic(string path)
        {
            _namespaceManager.DeleteTopic(path);
        }

        public void DeleteSubscription(string path, string subscriptionName)
        {
            _namespaceManager.DeleteSubscription(path, subscriptionName);
        }
    }
}
