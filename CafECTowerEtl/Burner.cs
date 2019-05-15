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
using System.Configuration;
using Microsoft.Azure.Documents;
//using Microsoft.Extensions.Configuration;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    public static class Burner
    {
        [FunctionName("Burner")]
        public static async Task Run(
            [BlobTrigger("ectower-cookeast/raw/Met/{name}", Connection = "ltarcafdatastreamConnectionString")]Stream myBlob,
            string name,
            TraceWriter log)
        {
        }
    }
}
