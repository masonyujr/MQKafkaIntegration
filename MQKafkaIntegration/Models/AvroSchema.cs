using Avro;
using Avro.Generic;
using System.IO;

namespace MyKafkaMQProject.Models
{
    public class AvroSchemaValidator
    {
        private readonly RecordSchema _schema;

        public AvroSchemaValidator(string schemaJson)
        {
            _schema = (RecordSchema)Schema.Parse(schemaJson);
        }

        public byte[] Serialize(MyPocoModel poco)
        {
            var record = new GenericRecord(_schema);
            record.Add("CorrelationId", poco.CorrelationId);
            record.Add("Timestamp", poco.Timestamp.ToString("O"));
            record.Add("Payload", poco.Payload);

            using (var stream = new MemoryStream())
            {
                var writer = new GenericDatumWriter<GenericRecord>(_schema);
                var encoder = new Avro.IO.BinaryEncoder(stream);
                writer.Write(record, encoder);
                return stream.ToArray();
            }
        }

        public bool ValidateSchema(byte[] avroData)
        {
            using (var stream = new MemoryStream(avroData))
            {
                var reader = new GenericDatumReader<GenericRecord>(_schema, _schema); // Pass both writer and reader schemas
                var decoder = new Avro.IO.BinaryDecoder(stream);
                var record = reader.Read(null, decoder);

                // Perform validation logic if necessary, return true if valid.
                return record != null;
            }
        }
    }
}
