using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.Common.ServiceBus
{
    public interface INamespaceManager
    {
        bool QueueExists(string path);
        QueueDescription CreateQueue(string path, int maxDeliveryCount);
        QueueDescription GetQueue(string path);
        void DeleteQueue(string path);
        string ConnectionString { get; }
    }
}
