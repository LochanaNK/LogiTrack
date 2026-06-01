using System.Net.Http.Headers;
using System.Text.Json;
using Confluent.Kafka;
using LogiTrack.Shared;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<Null, string>(config).Build();
Console.WriteLine("Truck Simulator Started. Press Ctrl+C to exit.");

var random = new Random();
var truckId = "TRUCK-001";

while (true)
{
    var telemetry = new VehicleTelemetry(
            truckId,
            Latitude: 40.7128 + (random.NextDouble() * 0.01),
            Longitude: -74.0060 + (random.NextDouble()* 0.01),
            SpeedMph: random.Next(45, 75),
            EngineTempFahreheit: random.Next(190, 230),
            TimeStamp: DateTimeOffset.UtcNow
        );

    var jsonPayload = JsonSerializer.Serialize(telemetry);
    var deliveryResult = await producer.ProduceAsync("vehicle-telemetry", new Message<Null, string>
    {
        Value = jsonPayload
    });
    Console.WriteLine($"Sent: {jsonPayload} to partition {deliveryResult.Partition}");
    await Task.Delay(1000);
}