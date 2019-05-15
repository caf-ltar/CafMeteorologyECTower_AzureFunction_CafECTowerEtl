using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net;
using Caf.Etl.Models.CosmosDBSqlApi.Measurement;
using System.Collections.Generic;
using Caf.Etl.Nodes.LtarDataPortal.Load;
using Caf.Etl.Nodes.CosmosDBSqlApi.Transform;
using Caf.Etl.Models.LtarDataPortal.CORe;
using Caf.Etl.Nodes.LtarDataPortal.Extract;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl.CosmosDBSqlApiMesurementToLtarDataPortalObservation
{
    public static class CosmosDBSqlApiMeasurementToLtarDataPortalObservation
    {
        [FunctionName("CosmosDBSqlApiMeasurementToLtarDataPortalObservation")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            //log.LogInformation("C# HTTP trigger function processed a request.");
            //
            //string name = req.Query["name"];
            //
            //string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;
            //
            //return name != null
            //    ? (ActionResult)new OkObjectResult($"Hello, {name}")
            //    : new BadRequestObjectResult("Please pass a name on the query string or in the request body");

            log.LogInformation($"Webhook was triggered!");

            int utcOffset = -8;

            string jsonContent = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(jsonContent);

            //log.Info("data: " + data.ToString());

            if (data == null |
                data.measurements[0].physicalQuantities[0] == null |
                data.recentFilePath == null |
                data.recentFileContent == null)
            {
                log.LogError("No data");

                return new BadRequestObjectResult("Data are null");
            }

            // Convert Measurements to CORe.Observation
            //MeasurementJsonExtractor extractor = new MeasurementJsonExtractor();
            List<MeasurementV2> measurements = 
                JsonConvert.DeserializeObject<MeasurementV2>(data.measurements);                                                                                                

            LtarDataPortalCOReTransformer transformer = new LtarDataPortalCOReTransformer();
            COReCsvStringWriter loader = new COReCsvStringWriter();

            //List<Measurement> measurements = extractor.ToMeasurements(data.measurements.ToString());
            List<Observation> observations = transformer.ToCOReObservations("CAF", "000", 'L', utcOffset, measurements);
            log.LogInformation("count: " + observations.Count);
            // Check if we're writing a new file
            string filename = loader.GetFilenamePstDateTime(observations[0]);
            //string filename = "cafMET001L_01_20170900_00.csv";
            string oldFileYYYYMM = data.recentFilePath.ToString().Substring(data.recentFilePath.ToString().Length - 15, 6);
            string newFileYYYYMM = filename.Substring(filename.Length - 15, 6);

            log.LogInformation("old: " + oldFileYYYYMM);
            log.LogInformation("new: " + newFileYYYYMM);

            // If files match then we need to append the data
            if (oldFileYYYYMM == newFileYYYYMM)
            {
                log.LogInformation("Files match, appending old data");
                COReCsvExtractor e = new COReCsvExtractor();
                //log.Info("Recent content: " + data.recentFileContent.ToString());
                List<Observation> oldObs = e.GetObservations(data.recentFileContent.ToString(), utcOffset);
                if (oldObs.Count > 0)
                {
                    oldObs.AddRange(observations);
                    observations = oldObs;
                }
            }

            // Now write the data to string and return
            string fileContent = loader.GetContentString(observations);
            //string fileContent = "foo";

            var returnObj = new { filename = filename, fileContent = fileContent };

            string result = JsonConvert.SerializeObject(returnObj);

            return new JsonResult(result);
        }
    }
}
