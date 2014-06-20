using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading.Tasks;

namespace DalSoft.Azure.Common.ServiceBus
{
    /// <summary>For unit test use only</summary>
    internal interface IServiceBusClientWrapper
    {
        void OnMessageAsync(Func<BrokeredMessage, Task> onMessageCallback, OnMessageOptions onMessageOptions);
        Task SendAsync(BrokeredMessage message);
        void Close();
    }
}
