using System.Text.Json;
using Confluent.Kafka;
using LogiTrack.Shared;

namespace LogiTrack.EmergencyResponder;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly string _workerInstanceId;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
        _workerInstanceId = $"Mechanic-{Random.Shared.Next(100, 999)}";
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "emergency-response-team",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe("incident-handling");

        _logger.LogInformation($"[{_workerInstanceId}] clocked in and waiting for dispatch...");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(cancellationToken);
                var incident = JsonSerializer.Deserialize<IncidentEvent>(result.Message.Value);


                _logger.LogWarning($"[{_workerInstanceId}] Dispatched to {incident?.VehicleId}! Issue: {incident?.Description}. (Partition: {result.Partition})");
                await Task.Delay(2000, cancellationToken);
            }
        }
        catch (OperationCanceledException) { consumer.Close(); }
    }
}