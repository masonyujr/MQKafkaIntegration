namespace MyKafkaMQProject.Models
{
    public class MyPocoModel
    {
        public string CorrelationId { get; set; }
        public DateTime Timestamp { get; set; }
        public byte[] Payload { get; set; }
    }
}
