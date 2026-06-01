using System;
using System.Collections.Generic;
using System.Text;

namespace LogiTrack.Shared;

public record IncidentEvent(
        string IncidentId,
        string VehicleId,
        string Description,
        string Severity,
        DateTimeOffset TimeStamp
    );