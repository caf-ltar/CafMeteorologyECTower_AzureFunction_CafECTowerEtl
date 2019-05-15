using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;
using System.Net.Http;
using System.Net;

namespace Caf.Projects.CafMeteorologyEcTower.CafECTowerEtl.BlobFileToMd5
{
    public static class BlobFileToMd5
    {
        [FunctionName("BlobFileToMd5")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation($"Webhook was triggered!");

            string jsonContent = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(jsonContent);

            // Use input string to calculate MD5 hash
            string input = data.fileContent;
            string output = "";
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    sb.Append(hashBytes[i].ToString("X2"));
                }
                output = sb.ToString().ToLower();
            }

            string newFilename = data.filename.ToString().Replace("csv", "md5");

            var returnObj = new { hash = output, filename = newFilename };
            string result = JsonConvert.SerializeObject(returnObj);

            //var response = new HttpResponseMessage(HttpStatusCode.OK);
            //response.Content = new StringContent(result, System.Text.Encoding.UTF8, "application/json");
            //return response;
            return new JsonResult(result);
            

            //log.LogInformation("C# HTTP trigger function processed a request.");
            //
            //string name = req.Query["name"];
            //
            //string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;
            //
            //return name != null
            //    ? (ActionResult)new OkObjectResult($"Hello, {name}")
            //    : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
        }
    }
}
