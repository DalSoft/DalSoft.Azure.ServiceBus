
using System;

namespace DalSoft.Azure.ServiceBus
{
    public sealed class Settings
    {
        private bool _isMaxDeliveryCountSet;
        private int _maxDeliveryCount;

        public Settings()
        {
            _maxDeliveryCount = 10; 
        }

        /// <summary>A message is automatically deadlettered after this number of unsuccessful deliveries. A unsuccessful delivery is a exception when processing the message using a pump or subscription.</summary>
        public int MaxDeliveryCount
        {
            get { return _maxDeliveryCount; }
            set
            {
                _maxDeliveryCount = value;
                _isMaxDeliveryCountSet = true;
            }
        }

        public bool RequireDuplicateDetection { get; set; }

        public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }

        internal void Vaildate<T>(INamespaceManager namespaceManager)
        {
            var queueDescription = namespaceManager.GetQueue(ServiceBusCommon<T>.GetName());
            if (
                (_isMaxDeliveryCountSet && queueDescription.MaxDeliveryCount != MaxDeliveryCount) ||
                queueDescription.RequiresDuplicateDetection != RequireDuplicateDetection ||
                (DuplicateDetectionHistoryTimeWindow.HasValue && queueDescription.DuplicateDetectionHistoryTimeWindow != DuplicateDetectionHistoryTimeWindow)
            )
                throw new InvalidOperationException(
                    "The Azure SDK 2.3 only lets you set the MaxDeliveryCount when first creating the Queue. For existing queues you will need to change the MaxDeliveryCount manually via the Azure portal.");
        }
    }
}
