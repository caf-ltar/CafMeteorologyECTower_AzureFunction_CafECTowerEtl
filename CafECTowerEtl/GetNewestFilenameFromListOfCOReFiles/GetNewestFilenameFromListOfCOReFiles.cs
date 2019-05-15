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

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl.GetNewestFilenameFromListOfCOReFiles
{
    public static class GetNewestFilenameFromListOfCOReFiles
    {
        [FunctionName("GetNewestFilenameFromListOfCOReFiles")]
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

            DateTime newestDT = DateTime.MinValue;
            string filePathNewest = "";
            foreach (var file in data)
            {
                string filename = file.Name;

                // Only process csv files
                if (Path.GetExtension(filename) != ".csv") continue;

                // Expect filename similar to: "cafMET000L_01_20090700_00.csv"
                string[] sections = filename.Split('_');
                if (sections.Length < 4) continue;

                string dateString = sections[2];

                log.LogInformation("dateString: " + dateString);

                // If day is "00" then remove it
                DateTime dt;

                if (dateString.Substring(dateString.Length - 2) == "00")
                {
                    dateString = dateString.Remove(dateString.Length - 2);
                    dt = DateTime.ParseExact(dateString, "yyyyMM", CultureInfo.InvariantCulture);
                }
                else
                {
                    dt = DateTime.ParseExact(dateString, "yyyyMMdd", CultureInfo.InvariantCulture);
                }

                log.LogInformation("dt: " + dt.ToString());

                if (dt > newestDT)
                {
                    log.LogInformation($"Found newer file.  Path = {file.Path}");
                    newestDT = dt;
                    filePathNewest = file.Path;
                }
            }

            log.LogInformation("filePathNewest: " + filePathNewest);

            var returnObj = new { filepath = filePathNewest };

            string result = JsonConvert.SerializeObject(returnObj);

            return new JsonResult(result);
        }
    }
}
