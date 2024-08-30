using IBM.XMS;
using MyKafkaMQProject.Models;
using Serilog;
using System.Collections.Generic;

namespace MyKafkaMQProject
{
    public class MQReader : IDisposable
    {
        private readonly IConnectionFactory factory;
        private readonly IConnection connection;
        private readonly ISession session;
        private readonly IMessageConsumer consumer;

        public MQReader(string queueName, string connectionString)
        {
            Console.WriteLine(value: "set up IBM Connection Factory................");
            factory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ).CreateConnectionFactory();
            factory.SetStringProperty(XMSC.WMQ_HOST_NAME, connectionString);
            connection = factory.CreateConnection();
            session = connection.CreateSession(false, AcknowledgeMode.AutoAcknowledge);
            IDestination destination = session.CreateQueue(queueName);
            consumer = session.CreateConsumer(destination);
        }

        public IEnumerable<MyPocoModel> BrowseMessages(int maxMessages = 10)
        {
            List<MyPocoModel> messages = new List<MyPocoModel>();
            int count = 0;
            Console.WriteLine("set up while loop ......");
            IMessage message;
            while (count < maxMessages && (message = consumer.ReceiveNoWait()) != null)
            {
                if (message is IBytesMessage bytesMessage)
                {
                    byte[] payload = new byte[bytesMessage.BodyLength];
                    int bytesRead = bytesMessage.ReadBytes(payload);

                    // Ensure the bytesRead matches expected length
                    if (bytesRead == payload.Length)
                    {
                        MyPocoModel poco = new MyPocoModel
                        {
                            CorrelationId = message.JMSCorrelationID,
                            Timestamp = DateTime.Now,
                            Payload = payload
                        };

                        messages.Add(poco);
                        count++;
                    }
                    else
                    {
                        Log.Warning("Expected to read {ExpectedLength} bytes, but read {ActualLength} bytes", payload.Length, bytesRead);
                    }
                }
            }

            return messages;
        }

        public void Dispose()
        {
            consumer?.Close();
            session?.Close();
            connection?.Close();
        }
    }
}
