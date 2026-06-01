using System;
using System.Collections.Generic;
using System.Text;

namespace LogiTrack.Shared;

public record VehicleTelemetry(
    string VehicleId,
    double Latitude,
    double Longitude,
    int SpeedMph,
    int EngineTempFahrenheit,
    DateTimeOffset TimeStamp
);
