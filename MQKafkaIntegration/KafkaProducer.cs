using Confluent.Kafka;
using MyKafkaMQProject.Models;
using Serilog;
using System.Threading.Tasks;

namespace MyKafkaMQProject
{
    public class KafkaProducer : IDisposable
    {
        private readonly IProducer<string, byte[]> producer;
        private readonly AvroSchemaValidator avroValidator;

        public KafkaProducer(string bootstrapServers, AvroSchemaValidator avroValidator)
        {
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };
            producer = new ProducerBuilder<string, byte[]>(config).Build();
            this.avroValidator = avroValidator;
        }

        public async Task ProduceAsync(MyPocoModel poco, string topic)
        {
            try
            {
                string key = $"{poco.CorrelationId}_{poco.Timestamp:O}";
                var value = avroValidator.Serialize(poco);

                if (avroValidator.ValidateSchema(value))
                {
                    var message = new Message<string, byte[]>
                    {
                        Key = key,
                        Value = value
                    };

                    await producer.ProduceAsync(topic, message);
                    Log.Information("Produced valid Avro message to Kafka topic {Topic}", topic);
                }
                else
                {
                    Log.Warning("Produced message did not pass Avro schema validation.");
                }
            }
            catch (ProduceException<string, byte[]> e)
            {
                Log.Error(e, "An error occurred while producing the message to Kafka");
            }
        }

        public void Dispose()
        {
            producer?.Flush();
            producer?.Dispose();
        }
    }
}
