using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctions2020.Models
{
    class DhtMeasurment
    {
        public string deviceId { set; get; }
        public float temperature { get; set; }
        public float humidity { get; set; }
        public long epochTime { set; get; }
    }
}