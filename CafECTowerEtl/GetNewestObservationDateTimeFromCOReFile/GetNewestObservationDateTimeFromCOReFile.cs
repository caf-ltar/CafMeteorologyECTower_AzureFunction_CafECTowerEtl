using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl.GetNewestObservationDateTimeFromCOReFile
{
    public static class GetNewestObservationDateTimeFromCOReFile
    {
        [FunctionName("GetNewestObservationDateTimeFromCOReFile")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation($"Webhook was triggered!");

            string jsonContent = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(jsonContent);

            //log.Info("data: " + data.ToString());

            if (data == null)
            {
                log.LogError("No data");

                return new BadRequestObjectResult("Data are null");
            }

            string fileContent = data.fileContent;
            DateTime newestDTUtc = DateTime.MinValue;

            using (StringReader reader = new StringReader(fileContent))
            {
                // Skip first line (the header)
                reader.ReadLine();

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    //CAF,000,2010-07-10T00:00-05:00,L,18.4,0.6,127,21,0,102.4,0,,,12.93,20.40
                    string[] measurements = line.Split(',');

                    // Get datetime
                    log.LogInformation("Date: " + measurements[2]);
                    DateTimeOffset dtOffset = DateTimeOffset.Parse(measurements[2], CultureInfo.InvariantCulture);
                    log.LogInformation("dto: " + dtOffset.ToString("s"));

                    DateTime dtUtc = dtOffset.UtcDateTime;

                    log.LogInformation("DateUtc: " + dtUtc.ToString("s"));

                    if (dtUtc > newestDTUtc) newestDTUtc = dtUtc;
                }
            }


            var returnObj = new
            {
                formattedDateTime = newestDTUtc.ToString("s") + "Z",
                year = newestDTUtc.Year,
                month = newestDTUtc.Month,
                day = newestDTUtc.Day,
                hour = newestDTUtc.Hour,
                minute = newestDTUtc.Minute,
                second = newestDTUtc.Second,
                millisecond = newestDTUtc.Millisecond
            };

            string result = JsonConvert.SerializeObject(returnObj);

            return new JsonResult(result);
        }
    }
}
