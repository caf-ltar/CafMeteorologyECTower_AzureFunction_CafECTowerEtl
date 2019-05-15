using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Caf.Etl.Models.LoggerNet.TOA5.DataTables;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using System;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
//using Microsoft.Extensions.Configuration;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    public static class LoggerNetMetToCosmosDBSqlApiMeasurementCookWest
    {
        private static Lazy<DocumentClient> lazyClient = 
            new Lazy<DocumentClient>(DocumentClientInitializer.InitializeDocumentClient());
        private static DocumentClient documentClient => lazyClient.Value;

        [FunctionName("LoggerNetMetToCosmosDBSqlApiMeasurementCookWest")]
        public static async Task Run(
            [BlobTrigger("ectower-cookwest/raw/Met/{name}", Connection = "ltarcafdatastreamConnectionString")]Stream myBlob, 
            string name, 
            ILogger log,
            ExecutionContext context)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            LoggerNetMetToCosmosDBSqlApiMeasurement<Meteorology> pipe =
                new LoggerNetMetToCosmosDBSqlApiMeasurement<Meteorology>(
                    myBlob,
                    name,
                    log,
                    "LoggerNetMetToCosmosDBSqlApiMeasurementCookWest",
                    $"ectower-cookwest/raw/Met/{name}",
                    900,
                    documentClient);

            await pipe.PipeItAsync();
        }
    }
}
