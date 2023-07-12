// See https://aka.ms/new-console-template for more information

using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

//AvroSchemaRegister().Wait();
AvroProducer().GetAwaiter().GetResult();
AvroConsumer();

async Task AvroSchemaRegister()
{
    // 註冊模式到Schema Registry
    var config = new SchemaRegistryConfig
    {
        Url = "http://localhost:8081" // 替換成你的Schema Registry URL
    };

    using var schemaRegistry = new CachedSchemaRegistryClient(config);
    var schema = await File.ReadAllTextAsync("User.avsc");
    var registeredSchemaId = await schemaRegistry.RegisterSchemaAsync("user", schema);
    Console.WriteLine($"registeredSchemaId:{registeredSchemaId}");
}

void AvroConsumer()
{
    var schemaRegistryConfig = new SchemaRegistryConfig()
    {
        Url = "http://localhost:8081"
    };
    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = "192.168.80.3:9092",
        GroupId = "avro-consumer"
    };
    const string topicName = "test-topic";
    CancellationTokenSource cts = new CancellationTokenSource();

    using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
    using var consumer =
        new ConsumerBuilder<string, User>(consumerConfig)
            .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync())
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .Build();
    consumer.Subscribe(topicName);

    try
    {
        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume(cts.Token);
                var user = consumeResult.Message.Value;
                Console.WriteLine(
                    $"key: {consumeResult.Message.Key}, user id: {user.id}, user name: {user.name}, user age: {user.age}");
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        consumer.Close();
    }
}

async Task AvroProducer()
{
    var producerConfig = new ProducerConfig()
    {
        BootstrapServers = "192.168.80.3:9092"
    };

    var schemaRegistryConfig = new SchemaRegistryConfig()
    {
        Url = "http://localhost:8081"
    };
    var avroSerializerConfig = new AvroSerializerConfig
    {
        BufferBytes = 100
    };
    const string topicName = "test-topic";
    CancellationTokenSource cts = new CancellationTokenSource();
    using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
    using var producer =
        new ProducerBuilder<string, User>(producerConfig)
            .SetValueSerializer(new AvroSerializer<User>(schemaRegistry, avroSerializerConfig))
            .Build();

    User user = new User
    {
        id = 1,
        name = "Yowko Tsai",
        age = 40
    };
    await producer
        .ProduceAsync(topicName, new Message<string, User> { Key = DateTimeOffset.Now.Ticks.ToString(), Value = user }) // produce 時會將 Avro schema 註冊至 Schema Registry，schema name 會是 {topicName}-value
        .ContinueWith(task =>
        {
            if (!task.IsFaulted)
            {
                Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");

                return;
            }

            Console.WriteLine($"error producing message: {task.Exception?.InnerException}");
        });
    cts.Cancel();
}