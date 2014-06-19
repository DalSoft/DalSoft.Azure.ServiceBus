using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.Common.ServiceBus
{
    public class NamespaceManager : INamespaceManager
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

        public bool QueueExists(string path) 
        {
            return _namespaceManager.QueueExists(path);
        }

        public QueueDescription CreateQueue(string path, int maxDeliveryCount)
        {
            return _namespaceManager.CreateQueue(new QueueDescription(path) { MaxDeliveryCount = maxDeliveryCount });
        }

        public void DeleteQueue(string path)
        {
            _namespaceManager.DeleteQueue(path);
        }
    }
}
