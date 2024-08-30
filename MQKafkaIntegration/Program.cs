using Serilog;
using MyKafkaMQProject.Models;

namespace MyKafkaMQProject
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()  // Make sure this is recognized after installing Serilog.Sinks.Console
                .CreateLogger();
           
            string queueName = "YOUR_QUEUE_NAME";
            string connectionString = "YOUR_MQ_CONNECTION_STRING";
            string kafkaBootstrapServers = "YOUR_KAFKA_BOOTSTRAP_SERVERS";
            string kafkaTopic = "YOUR_KAFKA_TOPIC";
            Console.WriteLine("set up IBM MQ connection strings...................");
            string avroSchemaJson = @"
            {
                ""type"": ""record"",
                ""name"": ""MyPocoModel"",
                ""fields"": [
                    { ""name"": ""CorrelationId"", ""type"": ""string"" },
                    { ""name"": ""Timestamp"", ""type"": ""string"" },
                    { ""name"": ""Payload"", ""type"": ""bytes"" }
                ]
            }";

            var avroValidator = new AvroSchemaValidator(avroSchemaJson);

            using (var mqReader = new MQReader(queueName, connectionString))
            using (var kafkaProducer = new KafkaProducer(kafkaBootstrapServers, avroValidator))
            {
                var messages = mqReader.BrowseMessages(10);

                foreach (var message in messages)
                {
                    await kafkaProducer.ProduceAsync(message, kafkaTopic);
                }
            }

            Log.CloseAndFlush();
        }
    }
}
