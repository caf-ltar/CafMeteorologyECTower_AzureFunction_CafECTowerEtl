using Caf.Etl.Models.CosmosDBSqlApi;
using Caf.Etl.Models.CosmosDBSqlApi.EtlEvent;
using Caf.Etl.Models.CosmosDBSqlApi.Measurement;
using Caf.Etl.Models.LoggerNet.TOA5;
using Caf.Etl.Nodes.CosmosDBSqlApi.Load;
using Caf.Etl.Nodes.LoggerNet.Extract;
using Caf.Etl.Nodes.LoggerNet.Mappers;
using Caf.Etl.Nodes.LoggerNet.Transform;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    public class LoggerNetMetToCosmosDBSqlApiMeasurement<T> where T : IObservation
    {
        private readonly Stream myBlob;
        private readonly string name;
        private readonly ILogger log;
        private readonly string functionName;
        private readonly string version;
        private readonly string blobPath;
        private readonly int timestep;
        private readonly DocumentClient client;

        public LoggerNetMetToCosmosDBSqlApiMeasurement(
            Stream myBlob,
            string name,
            ILogger log,
            string functionName,
            string blobPath,
            int timestep,
            DocumentClient client)
        {
            this.myBlob = myBlob;
            this.name = name;
            this.log = log;
            this.functionName = functionName;
            this.version = "2.0.0";
            this.blobPath = blobPath;
            this.timestep = timestep;
            this.client = client;
        }

        public async Task PipeItAsync()
        {
            EtlEvent etlEvent = new EtlEvent(
                "EtlEvent",
                "AzureFunction",
                "http://files.cafltar.org/data/schema/documentDb/v2/etlEvent.json",
                "CafMeteorologyEcTower",
                version, functionName,
                DateTime.UtcNow);
            etlEvent.Inputs.Add(blobPath);

            StreamReader reader = new StreamReader(myBlob);
            string contents = "";

            log.LogInformation("About to read contents");
            try
            {
                contents = reader.ReadToEnd();
            }
            catch (Exception e)
            {
                etlEvent.Logs.Add(
                    $"Error reading Blob: {e.Message}");
            }

            DocumentLoader loader = new DocumentLoader(
                client,
                "cafdb",
                "items");

            log.LogInformation("Created loader");
            if (!String.IsNullOrEmpty(contents))
            {
                try
                {
                    log.LogInformation("Attempting extract and transform");
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
                    log.LogInformation("Attempting load");

                    int docsLoaded = 0;
                    int docsError = 0;
                    foreach(MeasurementV2 measurement in measurements)
                    {
                        try
                        {
                            ResourceResponse<Document> result =
                                await loader.LoadNoReplace(measurement);
                            if (result.StatusCode == HttpStatusCode.Created)
                            {
                                etlEvent.Outputs.Add(result.Resource.Id);
                                docsLoaded++;
                            }
                            else { docsError++; }
                        }
                        catch (Exception e)
                        {
                            etlEvent.Logs.Add(
                                $"Error loading MeasurementV2: {e.Message}");
                            log.LogError($"Error loading MeasurementV2: {e.Message}");
                            docsError++;
                        }
                    }
                    log.LogInformation(
                        $"Loaded {docsLoaded.ToString()} MeasurementV2s.");
                    log.LogInformation(
                        $"Error loading {docsError.ToString()} MeasurementV2s.");
                    etlEvent.Logs.Add(
                        $"Loaded {docsLoaded.ToString()} MeasurementV2s");
                    etlEvent.Logs.Add(
                        $"Error loading {docsError.ToString()} MeasurementV2s");
                }
                catch (Exception e)
                {
                    etlEvent.Logs.Add(
                        $"Error in ETL pipeline: {e.Message}");
                    log.LogError($"Error in ETL pipeline: {e.Message}");
                    throw new Exception("Error in ETL pipeline", e);
                }
                finally
                {
                    log.LogInformation("Loading etlEvent to db");
                    etlEvent.DateTimeEnd = DateTime.UtcNow;
                    ResourceResponse<Document> result = await loader.LoadNoReplace(etlEvent);
                    log.LogInformation($"Result of writing EtlEvent: {result.StatusCode.ToString()}");
                }
            }
        }
    }
}
