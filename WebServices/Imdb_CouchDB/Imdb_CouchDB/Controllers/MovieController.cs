using Imdb_CouchDB.Models;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Imdb_CouchDB.Controllers
{
    [Route("api/[controller]")]
    public class MovieController : IMovieRepository
    {
        Stopwatch stopWatch = new Stopwatch();
        string url = "http://145.94.189.26:5984/genres/_find";
        [HttpGet("year")]
        public async Task<ActionResult> GetGenreInformationAsync(string name, long year)
        {
            var client = new HttpClient
            {
                BaseAddress = new Uri(url)
            };
            string jsonRequest = null;
            if (string.IsNullOrEmpty(name))
            {
                jsonRequest = string.Format("{{\"selector\": {{\"year\": {0}}},\"fields\":[\"genre\",\"number_of_movies\"]}}", year);
            }
            else
            {
                jsonRequest = string.Format("{{\"selector\": {{\"year\": {0},\"genre\": \"{1}\"}},\"fields\":[\"movies\"]}}", year, name);
            }           
            stopWatch.Start();
            var response = await client.PostAsync(url, new StringContent(jsonRequest, Encoding.UTF8, "application/json"));
            stopWatch.Stop();
            var result = new StreamReader(await response.Content.ReadAsStreamAsync()).ReadToEnd();
            File.AppendAllText(@"Metrics.txt", string.Format("1. {0} \n", stopWatch.ElapsedMilliseconds));
            return new JsonResult(result);
        }

        [HttpGet("yearrange")]
        public async Task<ActionResult> GetGenreInformationAsync(string name, int beginYear, int endYear)
        {
            var client = new HttpClient
            {
                BaseAddress = new Uri(url)
            };
            string jsonRequest = null;
            if (string.IsNullOrEmpty(name))
            {
                jsonRequest = string.Format("{{\"selector\": {{\"year\": {{\"$gte\": {0},\"$lte\": {1}}}}},\"fields\":[\"year\", \"genre\",\"number_of_movies\"]}}", beginYear, endYear);
            }
            else
            {
                jsonRequest = string.Format("{{\"selector\": {{\"year\": {{\"$gte\": {0},\"$lte\":{1}}},\"genre\": \"{2}\"}},\"fields\":[\"year\",\"movies\"]}}", beginYear, endYear, name);
            }
            stopWatch.Start();
            var response = await client.PostAsync(url, new StringContent(jsonRequest, Encoding.UTF8, "application/json"));
            stopWatch.Stop();
            var result = new StreamReader(await response.Content.ReadAsStreamAsync()).ReadToEnd();
            File.AppendAllText(@"Metrics.txt", string.Format("2. {0} \n", stopWatch.ElapsedMilliseconds));
            return new JsonResult(result);
        }

        [HttpGet("actor")]
        public async Task<ActionResult> GetActorInformationAsync(long id, string name)
        {
            var client = new HttpClient
            {
                BaseAddress = new Uri(url)
            };
            string jsonRequest = null;
            if (string.IsNullOrEmpty(name))
            {
                jsonRequest = string.Format("{{\"selector\": {{\"actorid\": {0}}},\"fields\":[\"fname\",\"lname\",\"mname\",\"gender\",\"movies\"]}}", id);
            }
            else
            {
                jsonRequest = string.Format("{{\"selector\": {{\"fname\":\"{0}\"}},\"fields\":[\"actorid\",\"fname\",\"lname\",\"mname\",\"gender\",\"movies\"]}}", name);
            }
            stopWatch.Start();
            var response = await client.PostAsync(url, new StringContent(jsonRequest, Encoding.UTF8, "application/json"));
            stopWatch.Stop();
            var result = new StreamReader(await response.Content.ReadAsStreamAsync()).ReadToEnd();
            File.AppendAllText(@"Metrics.txt", string.Format("3. {0} \n", stopWatch.ElapsedMilliseconds));
            return new JsonResult(result);
        }
        [HttpGet("id/{Id}")]
        public async Task<ActionResult> GetMovieInformationAsync(long id)
        {
            var client = new HttpClient
            {
                BaseAddress = new Uri(url)
            };
            string jsonRequest = null;
            jsonRequest = string.Format("{{\"selector\": {{\"movieid\": {0}}}}}", id);
            stopWatch.Start();
            var response = await client.PostAsync(url, new StringContent(jsonRequest, Encoding.UTF8, "application/json"));
            stopWatch.Stop();
            var result = new StreamReader(await response.Content.ReadAsStreamAsync()).ReadToEnd();
            File.AppendAllText(@"Metrics.txt", string.Format("4. {0} \n", stopWatch.ElapsedMilliseconds));
            return new JsonResult(result);
        }
        [HttpGet("title/{Title}")]
        public async Task<ActionResult> GetMovieInformationAsync(string title)
        {
            var client = new HttpClient
            {
                BaseAddress = new Uri(url)
            };
            string jsonRequest = null;
            jsonRequest = string.Format("{{\"selector\": {{\"title\": \"{0}\"}}}}", title);
            stopWatch.Start();
            var response = await client.PostAsync(url, new StringContent(jsonRequest, Encoding.UTF8, "application/json"));
            stopWatch.Stop();
            var result = new StreamReader(await response.Content.ReadAsStreamAsync()).ReadToEnd();
            File.AppendAllText(@"Metrics.txt", string.Format("5. {0} \n", stopWatch.ElapsedMilliseconds));
            return new JsonResult(result);
        }
        [HttpGet("partialtitle")]
        public async Task<ActionResult> GetMovieInformationFromPartialTitleAsync(string partialTitle)
        {
            var client = new HttpClient
            {
                BaseAddress = new Uri(url)
            };
            string jsonRequest = null;
            
            jsonRequest = string.Format("{\"selector\": {{\"title\": {{\"$regex\":  \"Revenge\"}},\"type\": 3}}}}", partialTitle);
            stopWatch.Start();
            var response = await client.PostAsync(url, new StringContent(jsonRequest, Encoding.UTF8, "application/json"));
            stopWatch.Stop();
            var result = new StreamReader(await response.Content.ReadAsStreamAsync()).ReadToEnd();
            File.AppendAllText(@"Metrics.txt", string.Format("6. {0} \n", stopWatch.ElapsedMilliseconds));
            return new JsonResult(result);
        }
    }
}
