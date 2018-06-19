using System.IO;
using Caf.Etl.Nodes.LoggerNet.Mappers;
using Caf.Etl.Nodes.LoggerNet.Transform;
using Caf.Etl.Models.CosmosDBSqlApi.EtlEvent;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using Caf.Etl.Models.CosmosDBSqlApi.Measurement;
using Caf.Etl.Nodes.LoggerNet.Extract;
using Caf.Etl.Models.LoggerNet.TOA5.DataTables;
using Caf.Etl.Models.LoggerNet.TOA5;
using Microsoft.Azure.Documents.Client;
using Caf.Etl.Nodes.CosmosDBSqlApi.Load;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    public static class LoggerNetFluxToCosmosDBSqlApiMeasurementCookEast
    {
        [FunctionName("LoggerNetFluxToCosmosDBSqlApiMeasurementCookEast")]
        public static async Task Run(
            [BlobTrigger("ectower-cookeast/raw/Flux/{name}", 
            Connection = "CookEastFluxConnectionString")]
            Stream myBlob, 
            string name, 
            TraceWriter log,
            ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            EtlEvent etlEvent = new EtlEvent(
                "EtlEvent",
                "AzureFunction",
                "http://files.cafltar.org/data/schema/documentDb/v2/etlEvent.json",
                "CafMeteorologyEcTower",
                "1.0", "LoggerNetFluxToCosmosDBSqlApiMeasurementCookEast",
                DateTime.UtcNow);
            etlEvent.Outputs = null;
            etlEvent.Inputs.Add($"ectower-cookeast/raw/Flux/{name}");
            etlEvent.Logs.Add($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            StreamReader reader = new StreamReader(myBlob);
            string contents = "";

            try
            {
                contents = reader.ReadToEnd();
            }
            catch(Exception e)
            {
                etlEvent.Logs.Add(
                    $"Error reading Blob: {e.Message}");
            }

            DocumentClient client = new DocumentClient(
                new Uri(
                    config["Values:AzureCosmosDBUri"]),
                    config["Values:AzureCosmosDBKey"]);

            DocumentLoader loader = new DocumentLoader(
                client,
                "cafdb",
                "items");

            if(!String.IsNullOrEmpty(contents))
            {
                try
                {
                    TOA5Extractor extractor = new TOA5Extractor(
                    name,
                    contents,
                    -8);

                    TOA5 fluxTable = extractor.GetTOA5<Flux>();

                    // TODO: Move strings and such to settings file
                    DocumentDbMeasurementV2Transformer transformer =
                        new DocumentDbMeasurementV2Transformer(
                            new MapFromFluxDataTableToCafStandards(),
                            "http://files.cafltar.org/data/schema/documentDb/v2/measurement.json",
                            etlEvent.Id, 
                            "Measurement", 
                            "CafMeteorologyEcTower", 
                            1800);                

                    List<MeasurementV2> measurements = 
                        transformer.ToMeasurements(fluxTable);

                    /// Using the bulkImport sproc doesn't provide much benefit since
                    /// most data tables will only have a few measurements with the
                    /// same partition key.  But it's better than nothing.
                        await loader.LoadBulk(measurements);
                }
                catch(Exception e)
                {
                    etlEvent.Logs.Add(
                        $"Error in ETL pipeline: {e.Message}");
                    throw new Exception("Error in ETL pipeline", e);
                }
                finally
                {
                    etlEvent.DateTimeEnd = DateTime.UtcNow;
                    await loader.LoadNoReplace(etlEvent);
                }
            }
        }
    }
}
