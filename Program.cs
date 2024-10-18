using Confluent.Kafka;
using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        // Producer example
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            for (int i = 0; i < 10; i++)
            {
                var deliveryReport = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = $"Hello Kafka {i}" });
                Console.WriteLine($"Delivered '{deliveryReport.Value}' to '{deliveryReport.TopicPartitionOffset}'");
            }
        }

        // Consumer example
        var consumerConfig = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build())
        {
            consumer.Subscribe("test-topic");

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
            }
        }
    }
}
