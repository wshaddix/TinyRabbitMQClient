TinyRabbitMQClient
==================

A highly opinionated, simple to use and narrowly focused RabbitMQ client for .Net

# Overview #
This is a super simple RabbitMQ client that is designed for a very narrowly scoped application design. It is built for Asp.Net Web Applications that want to publish commands (messages) to a RabbitMQ server and for a queue processing framework to consume those commands and publish events (messages) back to the RabbitMQ server.

# Constraints #
## Durability ##
All exchanges, queues and messages will be durable and not transient. The applications that this is built for need exchanges, queues and messages to be processed regardless of whether or not clients are connected at the time the commands and events are published. In addition, everything needs to survive a process interruption (cluster fail-over, service restart, etc)

## Availability ##
It is assumed that your RabbitMQ cluster is exposed via a load balancer of some kind (hardware or software) so that as more client connections are made the various back-end RabbitMQ nodes are used in your algorithm of choice. This client will not make any attempt to connect to multiple RabbitMQ instances.

## Command Messages ##
All commands are sent to a direct exchange that is named after the command's .Net type full name and delivered to a queue named "< exchange name >.incoming". The reason for this is because it supports the scale out pattern that I think is effective which is whenever a queue can't be processed in a timely manner by one consumer then we should be able to connect another consumer dynamically without having to touch or configure the publishing code. A RabbitMQ direct exchange supports this approach.

## Event Messages ##
All events are sent to a fanout exchange that is named after the event's .Net type full name and delivered to a queue named "< exchange name >.incoming". The reason for this is because it allows anyone in the world/enterprise that wants to know when something happened to subscribe to events and do whatever they need to do. It also allows you to add/remove subscribers independently from one another. A RabbitMQ fanout exchange supports this approach.

# Usage - Queuing Commands and Publishing Events#
1. Add a configuration value in your web.config to hold the connection string. The connection string should conform to the [AMQP URI Specification](http://www.rabbitmq.com/uri-spec.html)

2. Add code in your Application_Start() to connect to the RabbitMQ server
```
    protected void Application_Start()
    {
    	...
    	var amqpUri = ConfigurationManager.AppSettings["amqpUri"];
    	Globals.QueueClient = Client.Connect(amqpUri);
    }
```

3. Add code in your Application_End() to disconnect from the RabbitMQ server
```
	protected void Application_End()
    {
        // release the connection to the message queue
        if (null != Globals.QueueClient)
        {
            Globals.QueueClient.Disconnect();                
        }
    }
```

4. Anywhere in your application you can queue commands and publish events
```
    var command = new UserCreateCommand {Username="myuser"} 
    Globals.QueueClient.QueueCommand(command);
```