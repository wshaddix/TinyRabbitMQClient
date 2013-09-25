namespace TinyRabbitMQClient
{
    public class QueueConsumptionResult
    {
        public string ErrorId { get; set; }

        public string ErrorMessage { get; set; }

        public bool WasSuccessful { get; set; }
    }
}