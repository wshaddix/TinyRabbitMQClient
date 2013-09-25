using Common.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing.v0_9_1;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace TinyRabbitMQClient
{
    public class Client
    {
        /// <summary>
        /// The connection to the AMQP server
        /// </summary>
        private readonly IConnection _connection;

        /// <summary>
        /// Common.Logging log interface so that clients can choose whatever logging mechanism they
        /// want
        /// </summary>
        private readonly ILog _log;

        /// <summary>
        /// Sets up the connection to the AMQP server and also initializes the ILog that we'll use
        /// to log status messages
        /// </summary>
        /// <param name="uri">The URI to the AMQP server. Should conform to the AMQP URI
        /// Specification found at http://www.rabbitmq.com/uri-spec.html</param>
        /// <param name="log">An instance of Common.ILog - http://netcommon.sourceforge.net/</param>
        public Client(string uri, ILog log)
        {
            _log = log;
            var connectionFactory = new ConnectionFactory { Uri = uri, RequestedHeartbeat = 15 };

            // create a connection to the amqp server
            _log.Debug(m => m("Attempting to connect using uri:{0}", uri));
            _connection = connectionFactory.CreateConnection();
            _log.Debug(m => m("Connected to AMQP server"));
        }

        /// <summary>
        /// Used by clients that want to process messages from a queue
        /// </summary>
        /// <param name="exchangeName">The name of the exchange that the queue should be bound to.
        /// Needed to ensure that the messages don't get black-hold if no one is listening at the
        /// time the message is published</param>
        /// <param name="exchangeType">The type of the exchange. Needed to ensure that the messages
        /// don't get black-hold if no one is listening at the time the message is published</param>
        /// <param name="queueName">The name of the queue that you want to process.</param>
        /// <param name="consumingFunction">The function to execute on the message when it is
        /// dequeued. This returns a QueueConsumptionResult so that we know if the consumer was
        /// successful or not.</param>
        /// <param name="cancellationToken">A CancellationToken that we check between processing
        /// messages that lets us know if we should quit listening for messages or keep
        /// processing</param>
        public void ConsumeQueue<T>(string exchangeName, string exchangeType, string queueName, Func<T, QueueConsumptionResult> consumingFunction, CancellationToken cancellationToken, string routingKey)
        {
            // open a channel on the connection
            _log.Debug(m => m("Opening channel on the amqp connection to consume a queue"));
            using (var channel = _connection.CreateModel())
            {
                // ensure that the exchange exists. don't try to declare the default exchange
                if (!string.IsNullOrEmpty(exchangeName))
                {
                    _log.Debug(m => m("Ensuring that the {0} exchange of type {1} exists", exchangeName, exchangeType));
                    channel.ExchangeDeclare(exchangeName, exchangeType, true);
                }

                // ensure that the incoming queue exists
                _log.Debug(m => m("Ensuring that the {0} queue exists", queueName));
                channel.QueueDeclare(queueName, true, false, false, null);

                // bind the queue to the exchange
                _log.Debug(m => m("Ensuring that the {0} exchange is bound to the {1} queue with routingKey {2}", exchangeName, queueName, routingKey));
                channel.QueueBind(queueName, exchangeName, routingKey);

                // create a consumer
                var consumer = new QueueingBasicConsumer(channel);
                _log.Debug(m => m("Consuming queue {0}", queueName));
                channel.BasicConsume(queueName, false, consumer);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // wait for a message to enter the queue
                        _log.Debug(m => m("Waiting on a message from the {0} queue", queueName));
                        var e = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                        // extract the message body
                        var body = Encoding.UTF8.GetString(e.Body);

                        try
                        {
                            _log.Debug(m => m("Received message. Attempting to process"));
                            var typedMsg = JsonConvert.DeserializeObject<T>(body);

                            // let the handler have the message. We capture the
                            // QueueConsumptionResult it gives back so that we can further process
                            // the message
                            var queueConsumptionResult = consumingFunction(typedMsg);

                            if (queueConsumptionResult.WasSuccessful)
                            {
                                // acknowledge the message as processed, removing it from the queue
                                _log.Debug(m => m("Processing was successful. Acknowledging."));
                                channel.BasicAck(e.DeliveryTag, false);
                            }
                            else
                            {
                                _log.Debug(m => m("Processing failed. Moving to error exchange."));

                                // Tag the message as errored with a unique identifier
                                var errorId = Guid.NewGuid().ToString();
                                var headers = new Dictionary<object, object>
                                {
                                    {"ErrorId", errorId},
                                    {"ErrorMessage", queueConsumptionResult.ErrorMessage}
                                };

                                // Send the message to the error queue
                                var errorRoutingKey = queueName + ".Errors";
                                QueueMessage(e.Body, MessageType.Errors, errorRoutingKey, headers);

                                // dequeue the message removing it from the queue (the message is
                                // now in the error queue with the error in the header)
                                channel.BasicAck(e.DeliveryTag, false);

                                // we can't throw the exception because we have to keep processing
                                // more messages from the queue. Instead we log it as an error
                                _log.Error(m => m("Error processing message from queue: {0} ErrorId: {1}, ErrorMessage: {2}", queueName, errorId, queueConsumptionResult.ErrorMessage));
                            }

                            // if a reply-to queue was requested put the queueConsumptionResult
                            // there
                            if (!string.IsNullOrEmpty(e.BasicProperties.ReplyTo))
                            {
                                // serialize the result to json
                                var jsonMessage = JsonConvert.SerializeObject(queueConsumptionResult);

                                // convert the json formatted result to bytes
                                var msgBytes = Encoding.UTF8.GetBytes(jsonMessage);

                                // the default exchange is bound to every queue using the queue name
                                // as the routing key so send the message to it and assume that the
                                // client has already created the ReplyTo queue and is consuming it
                                // now
                                QueueMessage(msgBytes, MessageType.Reply, e.BasicProperties.ReplyTo, skipQueueCreation: true);
                            }
                        }
                        catch (Exception ex)
                        {
                            // Tag the message as errored with a unique identifier
                            var errorId = Guid.NewGuid().ToString();
                            var headers = new Dictionary<object, object>
                                {
                                    {"ErrorId", errorId},
                                    {"ErrorMessage", ex.Message}
                                };

                            // Send the message to the error queue
                            var errorRoutingKey = queueName + ".Errors";
                            QueueMessage(e.Body, MessageType.Errors, errorRoutingKey, headers);

                            // dequeue the message
                            channel.BasicAck(e.DeliveryTag, false);

                            // we can't throw the exception because we have to keep processing more
                            // messages from the queue. Instead we log it as an error
                            _log.Error(m => m("Error processing message from queue: {0} ErrorId: {1}, ErrorMessage: {2}", queueName, errorId, ex.Message));
                        }
                    }
                    catch (EndOfStreamException)
                    {
                        // the connection to the amqp server was lost
                        _log.Fatal(m => m("Connection to the AMQP server was lost while waiting to dequeue the next message"));
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Disconnects from the AMQP server
        /// </summary>
        public void Disconnect()
        {
            // This will release the connection to the Rabbit MQ server
            if (null != _connection && _connection.IsOpen)
            {
                _log.Debug(m => m("Disconnecting from the AMQP server"));
                _connection.Close(Constants.ReplySuccess, "Closing the connection");
                _log.Debug(m => m("Disconnected from the AMQP server"));
            }
        }

        /// <summary>
        /// Publishes a message to a fanout exchange
        /// </summary>
        /// <typeparam name="T">The .Net type of the event</typeparam>
        /// <param name="event">The event that should be published (.Net type that gets serialized
        /// to json)</param>
        public void PublishEvent<T>(T @event)
        {
            _log.Debug(m => m("Publishing event {0}", @event.GetType().FullName));
            QueueMessage(@event, MessageType.Events);
            _log.Debug(m => m("Published event {0}", @event.GetType().FullName));
        }

        /// <summary>
        /// Publishes a message to a direct exchange named "Commands" using the .Net type as the
        /// routing key
        /// </summary>
        /// <typeparam name="T">The .Net type of the command</typeparam>
        /// <param name="command">The command that should be published (.Net type that gets
        /// serialized to json)</param>
        /// <param name="replyTo">The queue that the response should be put in</param>
        public void QueueCommand<T>(T command, string replyTo = null)
        {
            _log.Debug(m => m("Queueing command {0}", command.GetType().FullName));
            QueueMessage(command, MessageType.Commands, replyTo);
            _log.Debug(m => m("Queued {0}", command.GetType().FullName));
        }

        public TResponse QueueCommandAndWaitForResponse<TCommand, TResponse>(TCommand command)
        {
            TResponse response = default(TResponse);

            // create a temporary queue
            var replyToQueue = CreateTemporaryQueue();

            // queue the command
            QueueCommand(command, replyToQueue);

            // consume the temporary queue
            var msg = GetResponse(replyToQueue);

            // convert the event message into it's .Net type
            response = JsonConvert.DeserializeObject<TResponse>(msg);

            // return the event to the client
            return response;
        }

        private string CreateTemporaryQueue()
        {
            // open a channel on the connection
            _log.Debug(m => m("Opening channel on the amqp connection to create a temporary queue"));
            using (var channel = _connection.CreateModel())
            {
                var name = Guid.NewGuid().ToString();
                // the queue cannot be exclusive because the queue processing framework that is
                // running on the back-end is using a different connection/channel to write to this
                // temporary queue. It will get cleaned up automatically because we specify it to be
                // an autodelete queue
                channel.QueueDeclare(name, false, false, true, null);
                return name;
            }
        }

        private byte[] FormatMessage<T>(T message)
        {
            // serialized the object as a json string
            var jsonMessage = JsonConvert.SerializeObject(message);

            // convert the json formatted message to bytes
            var msgBytes = Encoding.UTF8.GetBytes(jsonMessage);
            return msgBytes;
        }

        private string GetExchangeName(MessageType messageType)
        {
            var exchangeName = "Unknown";
            switch (messageType)
            {
                case MessageType.Commands:
                    exchangeName = "Commands";
                    break;

                case MessageType.Errors:
                    exchangeName = "Errors";
                    break;

                case MessageType.Events:
                    exchangeName = "Events";
                    break;

                case MessageType.Reply:
                    exchangeName = "";
                    break;
            }

            return exchangeName;
        }

        private string GetExchangeType(MessageType messageType)
        {
            var exchangeType = ExchangeType.Direct;
            switch (messageType)
            {
                case MessageType.Commands:
                    exchangeType = ExchangeType.Direct;
                    break;

                case MessageType.Errors:
                    exchangeType = ExchangeType.Direct;
                    break;

                case MessageType.Events:
                    exchangeType = ExchangeType.Topic;
                    break;

                case MessageType.Reply:
                    exchangeType = ExchangeType.Direct;
                    break;
            }

            return exchangeType;
        }

        private string GetResponse(string queueName)
        {
            // open a channel on the connection
            _log.Debug(m => m("Opening channel on the amqp connection to get a response from a temporary queue"));
            using (var channel = _connection.CreateModel())
            {
                // create a consumer
                var consumer = new QueueingBasicConsumer(channel);
                _log.Debug(m => m("Consuming queue {0}", queueName));
                channel.BasicConsume(queueName, false, consumer);

                try
                {
                    // wait for a message to enter the queue
                    _log.Debug(m => m("Waiting on a message from the {0} queue", queueName));
                    var e = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    // extract the message body
                    var body = Encoding.UTF8.GetString(e.Body);

                    // acknowledge the message as processed
                    _log.Debug(m => m("Dequeueing was successful. Acknowledging."));
                    channel.BasicAck(e.DeliveryTag, false);

                    return body;
                }
                catch (EndOfStreamException)
                {
                    // the connection to the amqp server was lost
                    _log.Fatal(m => m("Connection to the AMQP server was lost while waiting to dequeue the next message"));
                }
            }

            return string.Empty;
        }

        private string GetRoutingKey<T>(MessageType messageType)
        {
            // make the routing key the name of the .Net type and append a messageType specific
            // suffix so the same .Net type can have multiple queues based on the context
            // (Command/Event plus Errors)
            var routingKey = typeof(T).FullName;

            switch (messageType)
            {
                case MessageType.Errors:
                    routingKey += ".Errors";
                    break;
            }

            return routingKey;
        }

        private void QueueMessage<T>(T message, MessageType messageType, string replyTo = null)
        {
            var routingKey = GetRoutingKey<T>(messageType);

            // serialize the .Net type to a json string to be sent to the exchange
            var formattedMessage = FormatMessage(message);

            // queue the message
            QueueMessage(formattedMessage, messageType, routingKey, null, replyTo);
        }

        private void QueueMessage(byte[] message, MessageType messageType, string routingKey, IDictionary<object, object> headers = null, string replyTo = null, bool skipQueueCreation = false)
        {
            // open a channel on the connection
            _log.Debug(m => m("Opening channel on the amqp connection to publish a message"));
            using (var channel = _connection.CreateModel())
            {
                // figure out the name of the exchange to publish to
                var exchangeName = GetExchangeName(messageType);

                // figure out the type of exchange to publish to
                var exchangeType = GetExchangeType(messageType);

                // ensure that the exchange exists but don't try to re-declare the default exchange
                // or we'll get an exception!
                if (!string.IsNullOrEmpty(exchangeName))
                {
                    _log.Debug(m => m("Ensuring that the {0} exchange of type {1} exists", exchangeName, exchangeType));
                    channel.ExchangeDeclare(exchangeName, exchangeType, true);
                }

                // ensure that the incoming queue exists unless the client tells us not to (in the
                // case of a temporary queue has been created already by the client for use by rpc
                // style calls). We also don't want to create the queue if an event is being
                // published because it is a fanout queue and we may have no consumers to process
                // the events. If anyone is interested then their queue consuming frameworks will
                // create their queues
                if (!skipQueueCreation && messageType != MessageType.Events)
                {
                    _log.Debug(m => m("Ensuring that the {0} queue exists", routingKey));
                    channel.QueueDeclare(routingKey, true, false, false, null);

                    // bind the queue to the exchange
                    _log.Debug(m => m("Ensuring that the {0} exchange is bound to the {1} queue", exchangeName, routingKey));
                    channel.QueueBind(routingKey, exchangeName, routingKey);
                }

                // mark the message as persistent
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;

                // if the client requests a replyTo set it
                if (!string.IsNullOrEmpty(replyTo))
                {
                    properties.ReplyTo = replyTo;
                }

                // add any optional headers
                if (null != headers)
                {
                    _log.Debug(m => m("Adding {0} headers to the message", headers.Count));
                    properties.Headers = new Dictionary<object, object>();

                    foreach (var header in headers)
                    {
                        properties.Headers.Add(header.Key, header.Value);
                    }
                }

                // publish the message to the exchange
                _log.Debug(m => m("Publishing the message to the {0} exchange", exchangeName));
                channel.BasicPublish(exchangeName, routingKey, properties, message);
            }
        }
    }
}