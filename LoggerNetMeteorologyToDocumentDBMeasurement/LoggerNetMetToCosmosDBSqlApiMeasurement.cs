using Caf.Etl.Models.CosmosDBSqlApi.EtlEvent;
using Caf.Etl.Models.CosmosDBSqlApi.Measurement;
using Caf.Etl.Models.LoggerNet.TOA5;
using Caf.Etl.Models.LoggerNet.TOA5.DataTables;
using Caf.Etl.Nodes.CosmosDBSqlApi.Load;
using Caf.Etl.Nodes.LoggerNet.Extract;
using Caf.Etl.Nodes.LoggerNet.Mappers;
using Caf.Etl.Nodes.LoggerNet.Transform;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    public class LoggerNetMetToCosmosDBSqlApiMeasurement<T> where T : IObservation
    {
        private readonly Stream myBlob;
        private readonly string name;
        private readonly TraceWriter log;
        private readonly string functionName;
        private readonly string version;
        private readonly string blobPath;
        private readonly int timestep;
        private readonly DocumentClient client;

        public LoggerNetMetToCosmosDBSqlApiMeasurement(
            Stream myBlob,
            string name,
            TraceWriter log,
            string functionName,
            string blobPath,
            int timestep,
            DocumentClient client)
        {
            this.myBlob = myBlob;
            this.name = name;
            this.log = log;
            this.functionName = functionName;
            this.version = "1.1";
            this.blobPath = blobPath;
            this.timestep = timestep;
            this.client = client;
        }

        public async Task PipeItAsync()
        {
            //var config = new ConfigurationBuilder()
            //    .SetBasePath(context.FunctionAppDirectory)
            //    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            //    .AddEnvironmentVariables()
            //    .Build();

            EtlEvent etlEvent = new EtlEvent(
                "EtlEvent",
                "AzureFunction",
                "http://files.cafltar.org/data/schema/documentDb/v2/etlEvent.json",
                "CafMeteorologyEcTower",
                version, functionName,
                DateTime.UtcNow);
            etlEvent.Outputs = null;
            etlEvent.Inputs.Add(blobPath);

            StreamReader reader = new StreamReader(myBlob);
            string contents = "";

            log.Info("About to read contents");
            try
            {
                contents = reader.ReadToEnd();
            }
            catch (Exception e)
            {
                etlEvent.Logs.Add(
                    $"Error reading Blob: {e.Message}");
            }

            //DocumentClient client = new DocumentClient(
            //    new Uri(
            //        config["Values:AzureCosmosDBUri"]),
            //        config["Values:AzureCosmosDBKey"]);

            DocumentClient client;

            try
            {
                client = new DocumentClient(
                    new Uri(
                        ConfigurationManager.AppSettings["AzureCosmosDBUri"]),
                        ConfigurationManager.AppSettings["AzureCosmosDBKey"]);
            }
            catch (Exception e)
            {
                etlEvent.Logs.Add(
                    $"Error creating DocumentClient: {e.Message}");
                log.Error($"Error creating DocumentClient: {e.Message}");
                throw new Exception("Error creating DocumentClient", e);
            }


            DocumentLoader loader = new DocumentLoader(
                client,
                "cafdb",
                "items");

            log.Info("Created client and loader");
            if (!String.IsNullOrEmpty(contents))
            {
                try
                {
                    log.Info("Attempting extract and transform");
                    TOA5Extractor extractor = new TOA5Extractor(
                    name,
                    contents,
                    -8);

                    TOA5 metTable = extractor.GetTOA5<T>();

                    DocumentDbMeasurementV2Transformer transformer =
                        new DocumentDbMeasurementV2Transformer(
                            new MapFromToa5DataTableToCafStandards(),
                            "http://files.cafltar.org/data/schema/documentDb/v2/measurement.json",
                            etlEvent.Id,
                            "Measurement",
                            "CafMeteorologyEcTower",
                            timestep);

                    List<MeasurementV2> measurements =
                        transformer.ToMeasurements(metTable);
                    log.Info("Attempting load");
                    /// Using the bulkImport sproc doesn't provide much benefit since
                    /// most data tables will only have a few measurements with the
                    /// same partition key.  But it's better than nothing.
                    StoredProcedureResponse<bool>[] results = await loader.LoadBulk(measurements);
                    log.Info($"Loaded {results.Length.ToString()} measurements");
                    etlEvent.Logs.Add($"Loaded {results.Length.ToString()} measurements");

                }
                catch (Exception e)
                {
                    etlEvent.Logs.Add(
                        $"Error in ETL pipeline: {e.Message}");
                    log.Error($"Error in ETL pipeline: {e.Message}");
                    throw new Exception("Error in ETL pipeline", e);
                }
                finally
                {
                    log.Info("Loading etlEvent to db");
                    etlEvent.DateTimeEnd = DateTime.UtcNow;
                    ResourceResponse<Document> result = await loader.LoadNoReplace(etlEvent);
                    log.Info($"Result of writing EtlEvent: {result.StatusCode.ToString()}");
                }
            }
        }
    }
}
