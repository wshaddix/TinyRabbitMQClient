namespace TinyRabbitMQClient
{
    public class QueueConsumptionResult
    {
        public bool WasSuccessful { get; set; }
        public string ErrorId { get; set; }
        public string ErrorMessage { get; set; }
    }
}