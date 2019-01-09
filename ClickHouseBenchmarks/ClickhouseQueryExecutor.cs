using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace ClickHouseBenchmarks
{
    public class ListStringClickhouseResult
    {
        public string result { get; set; }
    }
    public class GzipWebClient : WebClient
    {
        protected override WebRequest GetWebRequest(Uri address)
        {
            HttpWebRequest request = base.GetWebRequest(address) as HttpWebRequest;
            request.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
            return request;
        }
    }
    public static class ClickhouseQueryExecutor
    {
        private const string host = "http://house.click:8123/";

        private static readonly GzipWebClient _webClient = new GzipWebClient();
        public static T ExecuteQuery<T>(string query) where T : class
        {
            _webClient.Headers[HttpRequestHeader.AcceptEncoding] = "gzip";
            var data = Encoding.ASCII.GetBytes(query + " FORMAT JSON");
            var response = _webClient.UploadData(host, data);
            var responseString = JObject.Parse(Encoding.Default.GetString(response));
            var responseBody = responseString["data"];
            var results = JsonConvert.DeserializeObject<T>(responseBody.ToString());
            return results;
        }
        public static byte[] ExecuteFileQuery(string query, string format = "TSV")
        {
            _webClient.Headers[HttpRequestHeader.AcceptEncoding] = "gzip";
            var data = Encoding.ASCII.GetBytes(query + $" FORMAT {format}");
            return _webClient.UploadData(host, data);
        }
        public static Stream ExecuteFileQueryStream(string query, string format = "TSV")
        {
            _webClient.Headers[HttpRequestHeader.AcceptEncoding] = "gzip";
            var data = Encoding.ASCII.GetBytes(query + $" FORMAT {format}");
            return new MemoryStream(_webClient.UploadData(host, data));
        }

        public static List<string> ReadAsStringArray(string query)
        {
            StreamReader sr = new StreamReader(ExecuteFileQueryStream(query, "RowBinary"));
            return sr.ReadToEnd().Split('*').Skip(1).ToList();
        }

    }
}
