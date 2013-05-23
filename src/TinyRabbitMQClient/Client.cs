using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.v0_9_1;
using System.Text;

namespace TinyRabbitMQClient
{
    public class Client
    {
        private readonly IConnection _connection;

        private Client(string uri)
        {
            var connectionFactory = new ConnectionFactory { Uri = uri };

            // create a connection to the amqp server
            // TODO: make this awaitable since it uses the network (don't want to wait on it)
            _connection = connectionFactory.CreateConnection();
        }

        public static Client Connect(string uri)
        {
            return new Client(uri);
        }

        public void Disconnect()
        {
            // This will release the connection to the Rabbit MQ server
            if (null != _connection && _connection.IsOpen)
            {
                _connection.Close(Constants.ReplySuccess, "Closing the connection");
            }
        }

        public void QueueCommand<T>(T command)
        {
            // TODO: Make this awaitable since we are talking over the network
            QueueMessage(command, ExchangeType.Direct);
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

                // publish the message to the exchange
                channel.BasicPublish(exchangeName, string.Empty, properties, msgBytes);
            }
        }
    }
}
