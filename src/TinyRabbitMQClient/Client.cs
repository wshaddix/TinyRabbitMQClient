using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing.v0_9_1;
using System.Text;

namespace TinyRabbitMQClient
{
    public class Client
    {
        private readonly ILogger _logger;
        private readonly IConnection _connection;

        public Client(string uri, ILogger logger)
        {
            _logger = logger;
            var connectionFactory = new ConnectionFactory { Uri = uri };

            // create a connection to the amqp server
            _logger.LogDebug("Attempting to connect using uri:{0}", uri);
            _connection = connectionFactory.CreateConnection();
            _logger.LogDebug("Connected to AMQP server");

        }

        public void Disconnect()
        {
            // This will release the connection to the Rabbit MQ server
            if (null != _connection && _connection.IsOpen)
            {
                _logger.LogDebug("Disconnecting from the AMQP server");
                _connection.Close(Constants.ReplySuccess, "Closing the connection");
                _logger.LogDebug("Disconnected from the AMQP server");
            }
        }

        public void QueueCommand<T>(T command)
        {
            QueueMessage(command, ExchangeType.Direct);
        }

        public void ConsumeQueue(string exchangeName, string exchangeType, string queueName, Func<string, QueueConsumptionResult> consumingAction, CancellationToken cancellationToken)
        {
            // open a channel on the connection
            using (var channel = _connection.CreateModel())
            {
                // ensure that the exchange exists
                channel.ExchangeDeclare(exchangeName, exchangeType, true);

                // ensure that the incoming queue exists
                channel.QueueDeclare(queueName, true, false, false, null);

                // bind the queue to the exchange
                channel.QueueBind(queueName, exchangeName, string.Empty);

                // create a consumer
                var consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume(queueName, false, consumer);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // wait for a message to enter the queue
                        var e = (BasicDeliverEventArgs) consumer.Queue.Dequeue();

                        // extract the message body
                        var body = Encoding.UTF8.GetString(e.Body);

                        try
                        {
                            // let the handler have the message
                            var result = consumingAction(body);

                            if (result.WasSuccessful)
                            {
                                // acknowledge the message as processed
                                channel.BasicAck(e.DeliveryTag, false);
                            }
                            else
                            {
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
                                _logger.LogError("Error processing message from queue: {0} ErrorId: {1}, ErrorMessage: {2}", queueName, errorId, result.ErrorMessage);
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
                            _logger.LogError("Error processing message from queue: {0} ErrorId: {1}, ErrorMessage: {2}", queueName, errorId, ex.Message);
                        }
                    }
                    catch (EndOfStreamException)
                    {
                        // the connection to the amqp server was lost
                        _logger.LogFatal("Connection to the AMQP server was lost while waiting to dequeue the next message");
                    }
                }
            }
        }
        
        public void PublishEvent<T>(T @event)
        {
            // TODO: Make this awaitable since we are talking over the network
            QueueMessage(@event, ExchangeType.Fanout);
        }

        private void QueueMessage<T>(T message, string exchangeType)
        {
            // TODO: Make this awaitable since we are talking over the network

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
            using (var channel = _connection.CreateModel())
            {
                // ensure that the exchange exists
                channel.ExchangeDeclare(exchangeName, exchangeType, true);

                // ensure that the incoming queue exists
                channel.QueueDeclare(queueName, true, false, false, null);

                // bind the queue to the exchange
                channel.QueueBind(queueName, exchangeName, string.Empty);

                // mark the message as persistent
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;
                
                // add any optional headers
                if (null != headers)
                {
                    properties.Headers = new Dictionary<object, object>();

                    foreach (var header in headers)
                    {
                        properties.Headers.Add(header.Key, header.Value);
                    }
                }

                // publish the message to the exchange
                channel.BasicPublish(exchangeName, string.Empty, properties, message);
            }
        }
    }
}
