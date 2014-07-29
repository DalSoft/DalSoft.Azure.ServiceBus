using System;
using Microsoft.ServiceBus.Messaging;

namespace DalSoft.Azure.ServiceBus
{
    /// <summary>
    /// Wraps any error thrown from the provided OnMessage callback. Message and innerException are set from the orignal exception 
    /// </summary>
    public class OnMessageException : Exception
    {
        public OnMessageException(BrokeredMessage brokeredMessage, string message, Exception ex) : base(message, ex)
        {
            BrokeredMessage = brokeredMessage;
        }

        public BrokeredMessage BrokeredMessage { get; private set; }
    }
}
