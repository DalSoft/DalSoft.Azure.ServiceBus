using Microsoft.ServiceBus.Messaging;
using System;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace DalSoft.Azure.Common.ServiceBus
{
    internal class ServiceBusCommon<TQueue> : IDisposable
    {
        private readonly IServiceBusWrapper _serviceBus;
        private readonly Func<IServiceBusWrapper> _onMessageClient;
        private CancellationTokenSource _cancellationTokenSource;
       
        internal ServiceBusCommon(IServiceBusWrapper serviceBus, Func<IServiceBusWrapper> onMessageClient) 
        {
            _serviceBus = serviceBus;
            _onMessageClient = onMessageClient;
        }

        // TODO we could use TMessage overload to support types, the pump would check the type and if it doesn't match TMessage it will Abandon for a competing pump to pick up
        /// <summary>
        /// Initiates the Message pump and the onMessage onError is invoked for each brokeredmessage that is received, disposing will close the client and will stop the pump.
        /// </summary>
        /// <param name="onMessage">Callback when the pump receives a brokeredMessage. *Do not swallow exceptions in your onMessage onError* as the pump will not be notified of the exception and the brokeredMessage will be marked as completed!</param>
        /// <param name="onError">Callback when the pump errors unexpectedly or an exception is thrown from your onMessage onError when a receiving a brokeredMessage. This is where you would log exceptions. It is optional if not provided exceptions will be swallowed but the brokeredMessage won't be marked as complete! *Only throw/re-throw exceptions* in this onError if you want the stop the pump receiving new messages (if your in any doubt log but swallow exceptions in this onError).</param>
        /// <param name="onMessageOptions">See MSDN documentation for onMessageOptions</param>
        /// <param name="cancellationTokenSource">Used to manually stop the pump. Alternatively just dispose and the pump is cancelled and cleaned up for you.</param>
        /// <exception cref="OnMessageException">Exception type that will be passed the the onError calling  receiving a brokeredMessage from your onMessage Callback</exception>
        /// <remarks>OnMessage may only be called once per instance.</remarks>
        public Task OnMessage(Func<dynamic, Task> onMessage, Action<Exception> onError, OnMessageOptions onMessageOptions, CancellationTokenSource cancellationTokenSource)
        {
            if (_cancellationTokenSource!=null)
                throw new InvalidOperationException("You can only start one pump at a time per instance");
            
            if (onMessage == null)
                throw new ArgumentNullException("onMessage", "You must provide a Task to be invoked when the pump receives a brokeredMessage");

            onMessageOptions = onMessageOptions ?? new OnMessageOptions();

            if (!onMessageOptions.AutoComplete)
                throw new InvalidOperationException("This OnMessage cannot work with OnMessageOptions.AutoComplete set to false");
            
            _cancellationTokenSource = cancellationTokenSource;

            var onMessageClient = _onMessageClient(); //manage pump lifecycle, allow pump restarts, on enqueueing from a cancelled pump

            return Task.Run(() =>
            {
                onMessageOptions.ExceptionReceived += (sender, args) => HandleOnMessageError(args.Exception, onError); //handle message failures thrown by our own onMessageCallback
                
                onMessageClient.OnMessageAsync(async receivedMessage =>
                {
                    try
                    {
                        await onMessage(receivedMessage.Clone().GetBody());
                    }
                    catch (Exception ex)
                    {
                        throw new OnMessageException(receivedMessage.Clone(), ex.Message, ex);
                        //When using AutoComplete We *must* throw here if we swallow exceptions it will not know an error has occured and will mark as complete/delete the brokeredMessage
                    }

                }, onMessageOptions);

                _cancellationTokenSource.Token.WaitHandle.WaitOne();
            }, cancellationTokenSource.Token)
            .ContinueWith(t =>
            {   
                if (t.Exception != null)
                    HandleOnMessageError(t.Exception, onError); //handle pump failures
                
                onMessageClient.Close(); //force the pump to stop as it may be sometime before the task actually cancels
            });
        }

        /// <summary>
        /// Adds a brokeredMessage to the queue.
        /// </summary>
        /// <param name="message">The brokeredMessage you want to add.</param>
        /// <param name="onError">Callback when adding the brokeredMessage to the queue errors unexpectedly. This is an optional onError if a onError isn't provided the exception will be thrown</param>
        /// <returns></returns>
        public Task Send<TMessage>(TMessage message, Action<AggregateException> onError) where TMessage : class, new() 
        {
            if (message == null)
                throw new ArgumentNullException("message", "Message can't be null");

            onError = onError ?? (exception => { throw exception; });

            return _serviceBus
                    .SendAsync(new BrokeredMessage(message) { ContentType = typeof(TMessage).AssemblyQualifiedName })
                    .ContinueWith(task => HandleEnqueueError(task.Exception, onError));
        }

        /// <summary>
        /// Adds a brokeredMessage to the queue.
        /// </summary>
        /// <typeparam name="TMessage">The messge</typeparam>
        /// <param name="brokeredMessage">The brokeredMessage you want to add.</param>
        /// <param name="onError">Callback when adding the brokeredMessage to the queue errors unexpectedly. This is an optional onError if a onError isn't provided the exception will be thrown</param>
        /// <returns></returns>
        public Task Send<TMessage>(BrokeredMessage brokeredMessage, Action<AggregateException> onError) where TMessage : class, new() 
        {
            if (brokeredMessage == null)
                throw new ArgumentNullException("brokeredMessage", "Message can't be null");

            if (brokeredMessage.ContentType != null)
                throw new ArgumentException("producer setting the ContentType is not supported");

            brokeredMessage.ContentType = typeof(TMessage).AssemblyQualifiedName;
            brokeredMessage.Clone().GetBody(); //Will throw if the TMessage and the actual type provided to the brokeredmessage differ

            return _serviceBus
                    .SendAsync(brokeredMessage)
                    .ContinueWith(task => HandleEnqueueError(task.Exception, onError));
        }

        public static string GetName()
        {
            var queueName = typeof(TQueue).FullName;

            if (queueName.Length > 260)
                throw new FormatException(string.Format("Queue name can't be > 260 characters. Make your namespace or class name shorter."));

            return queueName;
        }

        private static void HandleEnqueueError(AggregateException ex, Action<AggregateException> onError)
        {
            onError = onError ?? (exception => { throw exception; });
            
            if (ex == null)
                return;
            
            onError(ex);
        }

        private static void HandleOnMessageError(Exception ex, Action<Exception> onError)
        {
            onError = onError ?? (exception => { }); //see <param name="onError"> why we swallow should always exceptions here
            
            if (ex == null)  
                return;
            
            if (ex is OperationCanceledException) //Handle Task cancelling
                return;

            var aggregateException = ex as AggregateException;
            if (aggregateException != null) //Handle OnMessage failure called by ContinueWith 
            {
                if (aggregateException.InnerExceptions.Count == 1) //Usually there is only one exception causing the OnMessage failure so pass that if we can   
                {
                    onError(ex.InnerException);
                    return;
                }

                onError(aggregateException.Flatten());
                return;
            }

            onError(ex); //default error from onmessage onError
        }

        public void Dispose()
        {
            try
            {
                if (_cancellationTokenSource != null && !_cancellationTokenSource.IsCancellationRequested)
                    _cancellationTokenSource.Cancel();

                _serviceBus.Close();
            }   // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // If the client errors on closing we still need to dispose.  
            } 
        }
    }

    internal static class BrokeredMessageExtensions
    {
        public static object GetBody(this BrokeredMessage brokeredMessage)
        {
            if (string.IsNullOrWhiteSpace(brokeredMessage.ContentType))
                throw new InvalidOperationException("ContentType must be set to the Type of the brokeredMessage. Please send the brokeredMessage using DalSoft.Azure.ServiceBus.Queue");

            var serializer = new DataContractSerializer(Type.GetType(brokeredMessage.ContentType, true));
            using (var stream = brokeredMessage.GetBody<Stream>())
            using (var binaryReader = XmlDictionaryReader.CreateBinaryReader(stream, XmlDictionaryReaderQuotas.Max))
                return serializer.ReadObject(binaryReader);
        }
    }
}


