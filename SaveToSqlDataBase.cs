using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json;
using AzureFunctions2020.Models;

namespace SendDataToSqlDataBase
{
    public static class SaveToSqlDataBase
    {
        private static HttpClient client = new HttpClient();
        //private static readonly DeviceClient deviceClient = DeviceClient.CreateFromConnectionString("HostName=IOTHUB-2021.azure-devices.net;DeviceId=84:CC:A8:85:C2:BE;SharedAccessKey=kjCkAb62qLxH5QYr206/x0gEIAWlkMa5+jP0FNlM+CQ=", TransportType.Mqtt);

        [FunctionName("Function1")]
        public static void Run([IoTHubTrigger("messages/events", Connection = "IOTHUB-2021"/*, ConsumerGroup = "consumergroup"*/)] EventData message, ILogger log)
        {

            log.LogInformation($"Message: {Encoding.UTF8.GetString(message.Body.Array)}");
            var msg = JsonConvert.DeserializeObject<DhtMeasurment>(Encoding.UTF8.GetString(message.Body.Array));
            using (var conn = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnection")))
            {

                conn.Open();


                using (var cmd = new SqlCommand("", conn))
                {

                    // DeviceVendors 
                    //cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM DeviceVendors WHERE VendorName = @VendorName) INSERT INTO DeviceVendors OUTPUT inserted.Id VALUES(@VendorName) ELSE SELECT Id FROM DeviceVendors WHERE VendorName = @VendorName";
                    //cmd.Parameters.AddWithValue("@VendorName", message.Properties["vendor"].ToString());
                    //var vendorId = int.Parse(cmd.ExecuteScalar().ToString());

                    // DeviceModels 
                    //cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM DeviceModels WHERE ModelName = @ModelName)INSERT INTO DeviceModels OUTPUT inserted.Id VALUES(@ModelName, @VendorId) ELSE SELECT Id FROM DeviceModels WHERE ModelName = @ModelName";
                    //cmd.Parameters.AddWithValue("@ModelName", message.Properties["model"].ToString());
                    //cmd.Parameters.AddWithValue("@VendorId", vendorId);
                    //var modelId = int.Parse(cmd.ExecuteScalar().ToString());

                    //DeviceTypes 
                    //cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM DeviceTypes WHERE TypeName = @TypeName) INSERT INTO DeviceTypes OUTPUT inserted.Id VALUES(@TypeName) ELSE SELECT Id FROM DeviceTypes WHERE TypeName = @TypeName";
                    //cmd.Parameters.AddWithValue("@TypeName", message.Properties["deviceType"].ToString());
                    //var deviceTypeId = int.Parse(cmd.ExecuteScalar().ToString());

                    //Locations 
                    //cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM GeoLocations WHERE Latitude = @Latitude AND Longitude = @Longitude) INSERT INTO GeoLocations OUTPUT inserted.Id VALUES(@Latitude, @Longitude) ELSE SELECT Id FROM GeoLocations WHERE Latitude = @Latitude AND Longitude = @Longitude";
                    //cmd.Parameters.AddWithValue("@Latitude", message.Properties["latitude"].ToString());
                    //cmd.Parameters.AddWithValue("@Longitude", message.Properties["longitude"].ToString());
                    //var LocationId = long.Parse(cmd.ExecuteScalar().ToString());

                    // Devices 
                    //cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM Devices WHERE DeviceName = @DeviceName) INSERT INTO Devices OUTPUT inserted.Id VALUES(@DeviceName, @DeviceTypeId) ELSE SELECT Id FROM Devices WHERE DeviceName = @DeviceName";
                    //cmd.Parameters.AddWithValue("@DeviceName", message.Properties["DeviceName"].ToString());
                    //cmd.Parameters.AddWithValue("@DeviceTypeId", deviceTypeId);
                    //cmd.Parameters.AddWithValue("@LocationId", LocationId);
                    //cmd.Parameters.AddWithValue("@ModelId", modelId);
                    //var deviceId = int.Parse(cmd.ExecuteScalar().ToString());

                    // TemperatureAlerts 

                    //cmd.CommandText = "IF NOT EXISTS(SELECT Id FROM TemperatureAlerts WHERE Status = @Status) INSERT INTO TemperatureAlerts OUTPUT inserted.Id VALUES(@Status)ELSE SELECT Id FROM TemperatureAlerts WHERE Status = @Status";
                    //cmd.Parameters.AddWithValue("@Status", message.Properties["TemperatureAlert"].ToString());
                    //var TemperatureAlertId = int.Parse(cmd.ExecuteScalar().ToString());

                    //timeTable
                    //cmd.CommandText = " IF NOT EXISTS(SELECT 1 FROM TimeTable WHERE UnixUtcTime =@UnixUtcTime) INSERT INTO TimeTable OUTPUT inserted.UnixUtcTime VALUES(@UnixUtcTime) ELSE SELECT UnixUtcTime FROM UnixUtcTime WHERE UnixUtcTime =@UnixUtcTime";
                    //cmd.Parameters.AddWithValue("@UnixUtcTime", int.Parse(message.Properties["epochTime"].ToString()));
                    //var MeasureUnixTime = int.Parse(cmd.ExecuteScalar().ToString());

                    cmd.CommandText = "INSERT INTO DhtMeasurements OUTPUT inserted.Id VALUES(@DeviceId, @epochTime, @Temperature, @Humidity)";
                    //cmd.Parameters.AddWithValue("@DeviceId", deviceId);
                    cmd.Parameters.AddWithValue("@DeviceId", msg.deviceId);
                    cmd.Parameters.AddWithValue("@epochTime", msg.epochTime);
                    cmd.Parameters.AddWithValue("@Temperature", msg.temperature);
                    cmd.Parameters.AddWithValue("@Humidity", msg.humidity);
                    //cmd.Parameters.AddWithValue("@type", msg.type);
                    //cmd.Parameters.AddWithValue("@TemperatureAlerts", msg.TemperatureAlerts);
                    //cmd.Parameters.AddWithValue("@TemperatureAlert", TemperatureAlertId);

                    cmd.ExecuteNonQuery();


                    return;
                }
            }

        }

    }
}

