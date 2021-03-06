﻿using NUnit.Framework;

namespace DalSoft.Azure.ServiceBus.Test.Integration
{
    [TestFixture]
    public class NamespaceManagerTests 
    {
        [Test]
        public void Ctor_ConnectionStringProvided_ProvidedConnectionStringIsUsed()
        {
            const string endpoint = "Endpoint=sb://my-server.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NOT_A_REAL_KEY=";
            var namespaceManager = new NamespaceManager(endpoint);
            Assert.That(namespaceManager.ConnectionString, Is.EqualTo(endpoint));
        }
    }
}
