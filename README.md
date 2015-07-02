DalSoft.Azure.ServiceBus
====================

## Running the tests 

* Create a folder above the repo folder called DalSoft.Azure.ServiceBus.Test.Config
* In that folder create a config file called test.config
* Add the following config:

```xml
<appSettings>
    <!-- Service Bus specific app setings for messaging connections -->
    <add key="Microsoft.ServiceBus.ConnectionString" value="connection string value to your service bus endpoint on azure" />
</appSettings>
```
