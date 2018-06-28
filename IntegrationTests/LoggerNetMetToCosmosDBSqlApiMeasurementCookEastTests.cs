using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;
using Caf.Etl.Models.CosmosDBSqlApi.Measurement;
using Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Caf.Etl.Models.CosmosDBSqlApi;

namespace Caf.Projects.CafMeteorologyEcTower.IntegrationTests
{
    /// <summary>
    /// Integration tests requires Cosmos DB Emulator configured with:
    /// database = "cafdb", collection = "items"
    /// </summary>
    public class LoggerNetMetToCosmosDBSqlApiMeasurementCookEastTests
    {
        private string fileWithActualDataV2 =
            @"Assets/CookEastEcTower_Met_Raw_2018_06_27_1315.dat";
        private DocumentClient client;
        
        public LoggerNetMetToCosmosDBSqlApiMeasurementCookEastTests()
            :base()
        {
            //var config = new ConfigurationBuilder()
            //    .AddJsonFile("local.settings.json")
            //    .AddEnvironmentVariables()
            //    .Build();
            ConfigurationManager.AppSettings["AzureCosmosDBUri"] = "https://localhost:8081";
            ConfigurationManager.AppSettings["AzureCosmosDBKey"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            client = new DocumentClient(
                new Uri(
                    ConfigurationManager.AppSettings["AzureCosmosDBUri"]),
                    ConfigurationManager.AppSettings["AzureCosmosDBKey"]);

            // Setup, deletes all Measurements
            deleteAllDocuments(getAllMeasurements().ToList<IAmDocument>());
            deleteAllDocuments(getAllEtlEvents().ToList<IAmDocument>());
        }

        [Fact]
        public async void Run_ActualDataV2_CreatesExpectedNumberOfDocs()
        {
            var ex = new ExecutionContext();
            ex.FunctionAppDirectory = Environment.CurrentDirectory;

            // Act
            using (FileStream s = new FileStream(fileWithActualDataV2, FileMode.Open))
            {
                await LoggerNetMetToCosmosDBSqlApiMeasurementCookEast.Run(
                    s,
                    "CookEastEcTower_Met_Raw_2018_06_27_1315.dat",
                    new TraceWriterStub(TraceLevel.Verbose),
                    ex);
            }

            // Assert
            int actualMeasurements = getAllMeasurements().Count();
            int expectedMeasurements = 51;
            Assert.Equal(actualMeasurements, expectedMeasurements);

            int actualEtlEvents = getAllEtlEvents().Count();
            int expectedEtlEvents = 1;
            Assert.Equal(actualEtlEvents, expectedEtlEvents);
        }

        private string convertStreamToString(Stream stream)
        {
            string s;

            using (var reader = new StreamReader(stream, true))
            {
                s = reader.ReadToEnd();
            }

            return s;
        }

        private IQueryable<MeasurementV2> getAllMeasurements()
        {
            IQueryable<MeasurementV2> measurements =
                client.CreateDocumentQuery<MeasurementV2>(
                    UriFactory.CreateDocumentCollectionUri("cafdb", "items"),
                    new FeedOptions { EnableCrossPartitionQuery = true })
                    .Where(m => m.Type == "Measurement");
            return measurements;
        }
        private IQueryable<MeasurementV2> getAllEtlEvents()
        {
            IQueryable<MeasurementV2> events =
                client.CreateDocumentQuery<MeasurementV2>(
                    UriFactory.CreateDocumentCollectionUri("cafdb", "items"),
                    new FeedOptions { EnableCrossPartitionQuery = true })
                    .Where(m => m.Type == "EtlEvent");
            return events;
        }
        private bool deleteAllDocuments(List<IAmDocument> documents)
        {
            foreach(var doc in documents)
            {
                client.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(
                        "cafdb", "items", doc.Id),
                    new RequestOptions {
                        PartitionKey = new PartitionKey(doc.PartitionKey)
                    }).Wait();
            }

            return true;
        }
    }

    public class TraceWriterStub : TraceWriter
    {
        protected TraceLevel _level;
        protected List<TraceEvent> _traces;
        public string TraceString { get; set; }

        public TraceWriterStub(TraceLevel level) : base(level)
        {
            _level = level;
            _traces = new List<TraceEvent>();
        }

        public override void Trace(TraceEvent traceEvent)
        {
            _traces.Add(traceEvent);
            TraceString = traceEvent.Message;
        }

        public override string ToString()
        {
            return TraceString;
        }

        public List<TraceEvent> Traces => _traces;
    }
}
