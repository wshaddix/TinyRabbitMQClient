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
        /// Common.Logging log interface so that clients can choose whatever logging mechanism they want
        /// </summary>
        private readonly ILog _log;

        /// <summary>
        /// The connection to the AMQP server
        /// </summary>
        private readonly IConnection _connection;

        /// <summary>
        /// Sets up the connection to the AMQP server and also initializes the ILog that we'll use 
        /// to log status messages
        /// </summary>
        /// <param name="uri">The URI to the AMQP server. Should conform to the AMQP URI Specification 
        /// found at http://www.rabbitmq.com/uri-spec.html
        /// </param>
        /// <param name="log">An instance of Common.ILog - http://netcommon.sourceforge.net/ </param>
        public Client(string uri, ILog log)
        {
            _log = log;
            var connectionFactory = new ConnectionFactory { Uri = uri };

            // create a connection to the amqp server
            _log.Debug(m => m("Attempting to connect using uri:{0}", uri));
            _connection = connectionFactory.CreateConnection();
            _log.Debug(m => m("Connected to AMQP server"));

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
        /// Publishes a message to a direct exchange
        /// </summary>
        /// <typeparam name="T">The .Net type of the command</typeparam>
        /// <param name="command">The command that should be published (.Net type that gets serialized to json)</param>
        public void QueueCommand<T>(T command)
        {
            _log.Debug(m => m("Queueing command {0}", command.GetType().FullName));
            QueueMessage(command, ExchangeType.Direct);
            _log.Debug(m => m("Queued {0}", command.GetType().FullName));
        }

        /// <summary>
        /// Used by clients that want to process messages from a queue
        /// </summary>
        /// <param name="exchangeName">The name of the exchange that the queue should be bound to. Needed to ensure that the messages
        /// don't get black-hold if no one is listening at the time the message is published</param>
        /// <param name="exchangeType">The type of the exchange. Needed to ensure that the messages
        /// don't get black-hold if no one is listening at the time the message is published</param>
        /// <param name="queueName">The name of the queue that you want to process.</param>
        /// <param name="consumingFunction">The function to execute on the message when it is dequeued. This returns a QueueConsumptionResult so that we know
        /// if the consumer was successful or not.</param>
        /// <param name="cancellationToken">A CancellationToken that we check between processing messages that lets us know if we should quit listening for 
        /// messages or keep processing</param>
        public void ConsumeQueue(string exchangeName, string exchangeType, string queueName, Func<string, QueueConsumptionResult> consumingFunction, CancellationToken cancellationToken)
        {
            // open a channel on the connection
            _log.Debug(m => m("Opening channel on the amqp connection to consume a queue"));
            using (var channel = _connection.CreateModel())
            {
                // ensure that the exchange exists
                _log.Debug(m => m("Ensuring that the {0} exchange of type {1} exists", exchangeName, exchangeType));
                channel.ExchangeDeclare(exchangeName, exchangeType, true);

                // ensure that the incoming queue exists
                _log.Debug(m => m("Ensuring that the {0} queue exists", queueName));
                channel.QueueDeclare(queueName, true, false, false, null);

                // bind the queue to the exchange
                _log.Debug(m => m("Ensuring that the {0} exchange is bound to the {1} queue", exchangeName, queueName));
                channel.QueueBind(queueName, exchangeName, string.Empty);

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
                            // let the handler have the message
                            _log.Debug(m => m("Received message. Attempting to process"));
                            var result = consumingFunction(body);

                            if (result.WasSuccessful)
                            {
                                // acknowledge the message as processed
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
                                    {"ErrorMessage", result.ErrorMessage}
                                };

                                // Send the message to the error queue
                                QueueMessage(e.Body, exchangeName + ".Errors", exchangeType, queueName + ".Errors", headers);

                                // dequeue the message
                                channel.BasicAck(e.DeliveryTag, false);

                                // we can't throw the exception because we have to keep processing 
                                // more messages from the queue. Instead we log it as an error
                                _log.Error(m => m("Error processing message from queue: {0} ErrorId: {1}, ErrorMessage: {2}", queueName, errorId, result.ErrorMessage));
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
                            QueueMessage(e.Body, exchangeName + ".Errors", exchangeType, queueName + ".Errors", headers);

                            // dequeue the message
                            channel.BasicAck(e.DeliveryTag, false);

                            // we can't throw the exception because we have to keep processing 
                            // more messages from the queue. Instead we log it as an error
                            _log.Error(m => m("Error processing message from queue: {0} ErrorId: {1}, ErrorMessage: {2}", queueName, errorId, ex.Message));
                        }
                    }
                    catch (EndOfStreamException)
                    {
                        // the connection to the amqp server was lost
                        _log.Fatal(m => m("Connection to the AMQP server was lost while waiting to dequeue the next message"));
                    }
                }
            }
        }

        /// <summary>
        /// Publishes a message to a fanout exchange
        /// </summary>
        /// <typeparam name="T">The .Net type of the event</typeparam>
        /// <param name="event">The event that should be published (.Net type that gets serialized to json)</param>
        public void PublishEvent<T>(T @event)
        {
            _log.Debug(m => m("Publishing event {0}", @event.GetType().FullName));
            QueueMessage(@event, ExchangeType.Fanout);
            _log.Debug(m => m("Published event {0}", @event.GetType().FullName));
        }

        private void QueueMessage<T>(T message, string exchangeType)
        {
            var exchangeName = typeof(T).FullName;
            var queueName = string.Format("{0}.Incoming", exchangeName);

            // serialize the message to json
            var jsonMessage = JsonConvert.SerializeObject(message);

            // convert the json formatted message to bytes
            var msgBytes = Encoding.UTF8.GetBytes(jsonMessage);

            // queue the message
            QueueMessage(msgBytes, exchangeName, exchangeType, queueName);
        }

        private void QueueMessage(byte[] message, string exchangeName, string exchangeType, string queueName, IDictionary<object, object> headers = null)
        {
            // open a channel on the connection
            _log.Debug(m => m("Opening channel on the amqp connection to publish a message"));
            using (var channel = _connection.CreateModel())
            {
                // ensure that the exchange exists
                _log.Debug(m => m("Ensuring that the {0} exchange of type {1} exists", exchangeName, exchangeType));
                channel.ExchangeDeclare(exchangeName, exchangeType, true);

                // ensure that the incoming queue exists
                _log.Debug(m => m("Ensuring that the {0} queue exists", queueName));
                channel.QueueDeclare(queueName, true, false, false, null);

                // bind the queue to the exchange
                _log.Debug(m => m("Ensuring that the {0} exchange is bound to the {1} queue", exchangeName, queueName));
                channel.QueueBind(queueName, exchangeName, string.Empty);

                // mark the message as persistent
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;

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
                channel.BasicPublish(exchangeName, string.Empty, properties, message);
            }
        }
    }
}
