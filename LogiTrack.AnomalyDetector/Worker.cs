using System.Text.Json;
using Confluent.Kafka;
using LogiTrack.Shared;

namespace LogiTrack.AnomalyDetector;

public class Worker : BackgroundService
{
    private readonly ILogger _logger;
    public Worker(ILogger<Worker> logger) => _logger = logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "anomaly-detector-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var producerConfig = new ProducerConfig{ BootstrapServers = "localhost:9092" };
        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe("vehicle-telemetry");
        _logger.LogWarning("Anomaly detector listening for critical engine temps...");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                var telemetry = JsonSerializer.Deserialize<VehicleTelemetry>(result.Message.Value);

                if (telemetry != null && telemetry.EngineTempFahrenheit > 225)
                {
                    _logger.LogCritical($"CRITICAL: {telemetry.VehicleId} engine temp at {telemetry.EngineTempFahrenheit}! Raising incident...");

                    var incident = new IncidentEvent(
                            IncidentId: Guid.NewGuid().ToString(),
                            VehicleId: telemetry.VehicleId,
                            Description: $"Engine Overheating: {telemetry.EngineTempFahrenheit}F",
                            Severity: "HIGH",
                            TimeStamp: DateTimeOffset.UtcNow
                        );

                    await producer.ProduceAsync("incident-handling", new Message<string, string>{
                        Key = incident.VehicleId,
                        Value = JsonSerializer.Serialize(incident)
                    });
                }
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
}