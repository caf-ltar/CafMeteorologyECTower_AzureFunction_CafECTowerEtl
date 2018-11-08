using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Caf.Etl.Models.CosmosDBSqlApi.Measurement;
using Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Caf.Etl.Models.CosmosDBSqlApi;
using Caf.Etl.Models.CosmosDBSqlApi.EtlEvent;
using Microsoft.Extensions.Logging;
using Moq;

namespace Caf.Projects.CafMeteorologyEcTower.IntegrationTests
{
    /// <summary>
    /// Integration tests requires Cosmos DB Emulator configured with:
    /// database = "cafdb", collection = "items"
    /// </summary>
    public class LoggerNetMetToCosmosDBSqlApiMeasurementTests
    {
        private string fileWithActualDataV2CookEast =
            @"Assets/CookEastEcTower_Met_Raw_2018_06_27_1315.dat";
        private string fileWithActualDataV2CookWest =
            @"Assets/CookWestEcTower_Met_Raw_2018_06_28_1015.dat";
        private string fileWithActualDataV2BoydNorth =
            @"Assets/BoydNorthEcTower_Met_Raw_2018_06_28_1015.dat";
        private string fileWithActualDataV2BoydSouth =
            @"Assets/BoydSouthEcTower_Met_Raw_2018_06_28_1015.dat";
        private DocumentClient client;
        
        public LoggerNetMetToCosmosDBSqlApiMeasurementTests()
            :base()
        {
            client = DocumentClientInitializer.InitializeDocumentClient();

            // Setup, deletes all Measurements
            deleteAllDocuments(getAllMeasurements().ToList<IAmDocument>());
            deleteAllDocuments(getAllEtlEvents().ToList<IAmDocument>());
        }

        [Fact]
        public async void Run_ActualDataV2CookEast_CreatesExpectedNumberOfDocs()
        {
            var ex = new ExecutionContext();
            ex.FunctionAppDirectory = Environment.CurrentDirectory;

            // Act
            using (FileStream s = new FileStream(fileWithActualDataV2CookEast, FileMode.Open))
            {
                await LoggerNetMetToCosmosDBSqlApiMeasurementCookEast.Run(
                    s,
                    "CookEastEcTower_Met_Raw_2018_06_27_1315.dat",
                    Mock.Of<ILogger>(), 
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

        [Fact]
        public async void Run_ActualDataV2CookWest_CreatesExpectedNumberOfDocs()
        {
            var ex = new ExecutionContext();
            ex.FunctionAppDirectory = Environment.CurrentDirectory;

            // Act
            using (FileStream s = new FileStream(fileWithActualDataV2CookWest, FileMode.Open))
            {
                await LoggerNetMetToCosmosDBSqlApiMeasurementCookWest.Run(
                    s,
                    "CookWestEcTower_Met_Raw_2018_06_28_1015.dat",
                    Mock.Of<ILogger>(),
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

        [Fact]
        public async void Run_ActualDataV2BoydNorth_CreatesExpectedNumberOfDocs()
        {
            var ex = new ExecutionContext();
            ex.FunctionAppDirectory = Environment.CurrentDirectory;

            // Act
            using (FileStream s = new FileStream(fileWithActualDataV2BoydNorth, FileMode.Open))
            {
                await LoggerNetMetToCosmosDBSqlApiMeasurementBoydNorth.Run(
                    s,
                    "BoydNorthEcTower_Met_Raw_2018_06_28_1015.dat",
                    Mock.Of<ILogger>(),
                    ex);
            }

            // Assert
            int actualMeasurements = getAllMeasurements().Count();
            int expectedMeasurements = 31;
            Assert.Equal(actualMeasurements, expectedMeasurements);

            int actualEtlEvents = getAllEtlEvents().Count();
            int expectedEtlEvents = 1;
            Assert.Equal(actualEtlEvents, expectedEtlEvents);
        }

        [Fact]
        public async void Run_ActualDataV2BoydSouth_CreatesExpectedNumberOfDocs()
        {
            var ex = new ExecutionContext();
            ex.FunctionAppDirectory = Environment.CurrentDirectory;

            // Act
            using (FileStream s = new FileStream(fileWithActualDataV2BoydSouth, FileMode.Open))
            {
                await LoggerNetMetToCosmosDBSqlApiMeasurementBoydSouth.Run(
                    s,
                    "BoydSouthEcTower_Met_Raw_2018_06_28_1015.dat",
                    Mock.Of<ILogger>(),
                    ex);
            }

            // Assert
            int actualMeasurements = getAllMeasurements().Count();
            int expectedMeasurements = 31;
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
        private IQueryable<EtlEvent> getAllEtlEvents()
        {
            IQueryable<EtlEvent> events =
                client.CreateDocumentQuery<EtlEvent>(
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
}
