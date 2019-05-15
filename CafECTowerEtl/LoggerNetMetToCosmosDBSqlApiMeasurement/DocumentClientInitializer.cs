using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl
{
    static public class DocumentClientInitializer
    {
        public static DocumentClient InitializeDocumentClient()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            JsonSerializerSettings serializerSettings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };

            string dbUri = config["AzureCosmosDBUri"] ?? config["Values:AzureCosmosDBUri"];
            string dbKey = config["AzureCosmosDBKey"] ?? config["Values:AzureCosmosDBKey"];

            DocumentClient client = new DocumentClient(
                new Uri(
                    dbUri),
                    dbKey,
                    serializerSettings);

            return client;
        }
    }
}
