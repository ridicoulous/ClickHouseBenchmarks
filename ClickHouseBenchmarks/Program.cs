using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace ClickHouseBenchmarks
{
    class Program
    {
        static string clickhouseConnectionString = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=house.click;Port=9000;User=admin;Password=admin;SocketTimeout=600000;Database=database;";
        static Stopwatch sw = new Stopwatch();
        static void Log(string message, int count)
        {
            Console.WriteLine($"{message} getted {count} records by \t\t {sw.ElapsedMilliseconds} ms.");
            sw.Restart();
        }
        static void Main(string[] args)
        {
            for (int i = 1; i <= 131072; i *= 2)
            {
                string query = $"SELECT number as result FROM system.numbers LIMIT {i}";
                Test(query);
            }
        }

        public static void Test(string query)
        {
            sw.Start();
            int parseJson = ClickhouseQueryExecutor.ExecuteQuery<List<ListStringClickhouseResult>>(query).Count;
            Log("ParseJson", parseJson);
            int parseTSV = ParseTSV(query);
            Log("ParseTSV", parseTSV);
            int parseRowBinary = ParseRowBinary(query);
            Log("ParseBin", parseRowBinary);
            int readAll = ReadAll(query);
            Log("ReadAll", readAll);

        }
        private static ClickHouseConnection GetConnection()
        {
            var settings = new ClickHouseConnectionSettings(clickhouseConnectionString);
            var cnn = new ClickHouseConnection(settings);            
            cnn.Open();
            return cnn;
        }
        public static int ReadAll(string query)
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand(query);
                var list = new List<List<object>>();

                using (var reader = cmd.ExecuteReader())
                {
                    // times.Add(sw.Elapsed);
                    //sw.Restart();
                    reader.ReadAll(x =>
                    {
                        var rowList = new List<Object>();
                        for (var i = 0; i < x.FieldCount; i++)
                            rowList.Add(x.GetValue(i));
                        list.Add(rowList);
                        //  times.Add(sw.Elapsed);
                        // sw.Restart();
                    });

                }
                return list.Count;
            }
        }
        private static int ParseTSV(string query)
        {
            StreamReader sr = new StreamReader(ClickhouseQueryExecutor.ExecuteFileQueryStream(query));
            List<string[]> parsedValues = new List<string[]>();
            string readLine;
            while ((readLine = sr.ReadLine()) != null)
            {
                string[] parsedValue = readLine.Split('\t');
                parsedValues.Add(parsedValue);
            }
            return parsedValues.Count;
        }
        private static int ParseRowBinary(string query)
        {
            return ClickhouseQueryExecutor.ReadAsStringArray(query).Count;
        }

    }
}
