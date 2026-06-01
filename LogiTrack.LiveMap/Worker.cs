using System.Text.Json;
using Confluent.Kafka;
using LogiTrack.Shared;

namespace LogiTrack.LiveMap;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stopToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "live-map-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe("vehicle-telemetry");

        _logger.LogInformation("Live map consumer started.");
        try
        {
            while (!stopToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stopToken);
                var telemetry = JsonSerializer.Deserialize<VehicleTelemetry>(consumeResult.Message.Value);

                _logger.LogInformation($"[Map Update] {telemetry?.VehicleId} is at {telemetry?.Latitude}, {telemetry?.Longitude} doing {telemetry?.SpeedMph} MPH.");

            }
        }
        catch(Exception ex)
        {
            consumer.Close();
            _logger.LogError(ex, "Error consuming messages.");
        }
    }
}