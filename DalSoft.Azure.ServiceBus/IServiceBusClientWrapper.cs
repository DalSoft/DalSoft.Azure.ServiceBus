using System;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.ServiceBus
{
    /// <summary>For unit test use only</summary>
    internal interface IServiceBusClientWrapper
    {
        void OnMessageAsync(Func<BrokeredMessage, Task> onMessageCallback, OnMessageOptions onMessageOptions);
        Task SendAsync(BrokeredMessage message);
        void Close();
    }
}
