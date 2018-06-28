using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Caf.Etl.Models.LoggerNet.TOA5.DataTables;
using System.Threading.Tasks;
using System.Configuration;
using System;
using Microsoft.Azure.Documents.Client;
//using Microsoft.Extensions.Configuration;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    public static class LoggerNetMetToCosmosDBSqlApiMeasurementBoydSouth
    {
        private static Lazy<DocumentClient> lazyClient = new Lazy<DocumentClient>(InitializeDocumentClient);
        private static DocumentClient documentClient => lazyClient.Value;

        private static DocumentClient InitializeDocumentClient()
        {
            return new DocumentClient(
                    new Uri(
                        ConfigurationManager.AppSettings["AzureCosmosDBUri"]),
                        ConfigurationManager.AppSettings["AzureCosmosDBKey"]);
        }

        [FunctionName("LoggerNetMetToCosmosDBSqlApiMeasurementBoydSouth")]
        public static async Task Run(
            [BlobTrigger("ectower-boydsouth/raw/Met/{name}", Connection = "ltarcafdatastreamConnectionString")]Stream myBlob, 
            string name, 
            TraceWriter log,
            ExecutionContext context)
        {
            log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            LoggerNetMetToCosmosDBSqlApiMeasurement<Meteorology> pipe =
                new LoggerNetMetToCosmosDBSqlApiMeasurement<Meteorology>(
                    myBlob,
                    name,
                    log,
                    "LoggerNetMetToCosmosDBSqlApiMeasurementBoydSouth",
                    $"ectower-boydsouth/raw/Met/{name}",
                    900,
                    documentClient);

            await pipe.PipeItAsync();
        }
    }
}
