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
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl.CosmosDBSqlApiMesurementToLtarDataPortalObservation;

namespace Caf.Projects.CafMeteorologyEcTower.IntegrationTests
{
    /// <summary>
    /// Integration tests requires Cosmos DB Emulator configured with:
    /// database = "cafdb", collection = "items"
    /// </summary>
    public class CosmosDBSqlApiMeasurementToLtarDataPortalObservationTests
    { 
        [Fact]
        public async void Run_ActualMeasurementV2Data_Success()
        {
            // Arrange
            string json = File.ReadAllText("Assets/validJson.json");
            Mock<HttpRequest> mockRequest = CreateMockRequest(json);

            var result = await CosmosDBSqlApiMeasurementToLtarDataPortalObservation.Run(
                mockRequest.Object, new Mock<ILogger>().Object);

            // Incomplete - was just trying to get it to run (bad TDD!)
        }

        // From: http://dontcodetired.com/blog/post/Mocking-HttpRequest-Body-Content-When-Testing-Azure-Function-HTTP-Trigger-Functions
        private static Mock<HttpRequest> CreateMockRequest(object body)
        {
            var ms = new MemoryStream();
            var sw = new StreamWriter(ms);

            var json = JsonConvert.SerializeObject(body);

            sw.Write(json);
            sw.Flush();

            ms.Position = 0;

            var mockRequest = new Mock<HttpRequest>();
            mockRequest.Setup(x => x.Body).Returns(ms);

            return mockRequest;
        }

        private static Mock<HttpRequest> CreateMockRequest(string body)
        {
            var ms = new MemoryStream();
            var sw = new StreamWriter(ms);

            var json = body;

            sw.Write(json);
            sw.Flush();

            ms.Position = 0;

            var mockRequest = new Mock<HttpRequest>();
            mockRequest.Setup(x => x.Body).Returns(ms);

            return mockRequest;
        }
    }
}
