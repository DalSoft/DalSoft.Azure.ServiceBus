using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace DalSoft.Azure.ServiceBus.CloudServices.WorkerRole
{
    //Based on the Compute Resource Consolidation Pattern article by the Microsoft Patterns and Practices team http://msdn.microsoft.com/en-us/library/dn589778.aspx
    public abstract class TaskWorker : RoleEntryPoint
    {
        // The cancellation token source used to cooperatively cancel running tasks.
        private CancellationTokenSource _cancellationTokenSource;
        // List of tasks running on the role instance.
        private List<Task> _tasks;
        
        public abstract List<Task> MyTasks();
        public abstract CancellationTokenSource MyCancellationTokenSource();

        public override bool OnStart()
        {
            ServicePointManager.DefaultConnectionLimit = 96;
            return base.OnStart();
        }

        public override void Run()
        {
            _cancellationTokenSource = MyCancellationTokenSource();
            _tasks = MyTasks();
            
            if (_cancellationTokenSource == null)
                throw new InvalidOperationException("You must override MyCancellationTokenSource and return a CancellationTokenSource that will be used to cancel MyTasks");

            if (_tasks == null || _tasks.Count < 1)
                throw new InvalidOperationException("You must override MyTasks and return at least one task to run");

            Trace.TraceInformation("Worker host tasks started");
            // The assumption is that all tasks should remain running and not return, 
            // similar to role entry Run() behavior.
            try
            {
                Task.WaitAny(_tasks.ToArray());
            }
            catch (AggregateException ex)
            {
                Trace.TraceError(ex.Message);

                // If any of the inner exceptions in the aggregate exception 
                // are not cancellation exceptions then re-throw the exception.
                ex.Handle(innerEx => (innerEx is OperationCanceledException));
            }
        }

        public override sealed void OnStop()
        {
            // Run has returned or role instance has been taken offline by Windows Azure, If there was not a cancellation request
            // An alternative to cancelling and returning when a task exits would be to restart the task.
            if (_cancellationTokenSource.IsCancellationRequested) //Cancel already requested exit
                return;

            Trace.TraceInformation("Task returned without cancellation request");
            Stop(TimeSpan.FromSeconds(30)); //cancel all tasks with a Timeout of 30 seconds
        }

        // Stop running tasks and wait for tasks to complete before returning 
        // unless the timeout expires.
        private void Stop(TimeSpan timeout)
        {
            Trace.TraceInformation("Stop called. Cancelling tasks.");
            // Cancel running tasks.
            _cancellationTokenSource.Cancel();

            Trace.TraceInformation("Waiting for canceled tasks to finish and return");

            // Wait for all the tasks to complete before returning. Note that the 
            // emulator currently allows 30 seconds and Azure allows five
            // minutes for processing to complete.
            try
            {
                Task.WaitAll(_tasks.ToArray(), timeout);
            }
            catch (AggregateException ex)
            {
                Trace.TraceError(ex.Message);

                // If any of the inner exceptions in the aggregate exception 
                // are not cancellation exceptions then re-throw the exception.
                ex.Handle(innerEx => (innerEx is OperationCanceledException));
            }
        }
    }
}
