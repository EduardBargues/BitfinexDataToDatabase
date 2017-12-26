using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitFinexDataToDatabase
{
    class Program
    {
        private static DateTime date1970 = new DateTime(1970, 1, 1);
        private static string databaseConnectionString = @"Server=localhost\SQLEXPRESS;Database=FinancialFreedom";//Trusted_Connection=True;";
        private static string userName = "sa";
        private static string password = "112358134711";

        static void Main(string[] args)
        {
            DateTime databaseMaxDate = GetMaxDateOnDatabase();
            GetDataFromBitFinexSince(databaseMaxDate.AddMilliseconds(1), DateTime.Today);
        }

        private static DateTime GetMaxDateOnDatabase()
        {
            DateTime maxDate = date1970;
            SecureString securePassword = new SecureString();
            foreach (char c in password)
                securePassword.AppendChar(c);
            securePassword.MakeReadOnly();
            SqlCredential credentials = new SqlCredential(userName, securePassword);
            using (SqlConnection connection = new SqlConnection(databaseConnectionString, credentials))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(
                    cmdText: "select max(Instant) from BitCoinTrades",
                    connection: connection);
                using (command)
                {
                    command.CommandType = CommandType.Text;
                    if (command.ExecuteScalar() is DateTime time)
                        maxDate = time;
                }
                connection.Close();
            }
            return maxDate;
        }

        private static void GetDataFromBitFinexSince(DateTime startDate, DateTime endDate)
        {
            const int limit = 1000;
            long first = (long)GetMillisecondsSince1970(startDate);
            long last = (long)GetMillisecondsSince1970(endDate);
            bool keepAsking = true;
            while (keepAsking)
                using (WebClient client = new WebClient())
                {
                    string requestUrl = GetRequestUrl(first, last, limit, true);
                    Console.WriteLine(requestUrl);

                    string data = GetWebData(client, requestUrl);
                    List<Trade> trades = new List<Trade>();
                    if (data != "[]")
                    {
                        trades = ParseData(data);
                        WriteDataToDatabase(trades);
                    }

                    DateTime maxDate = trades.Any()
                        ? trades.Max(trade => trade.Instant).AddMilliseconds(1)
                        : endDate;
                    first = (long)GetMillisecondsSince1970(maxDate);
                    Console.WriteLine($"{maxDate:G} - {endDate:G}");

                    keepAsking = maxDate < endDate;
                }
        }

        private static string GetWebData(WebClient client, string requestUrl)
        {
            bool success = false;
            string data = "";
            while (!success)
                try
                {
                    data = client.DownloadString(requestUrl);
                    success = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Thread.Sleep(TimeSpan.FromMinutes(2));
                    success = false;
                }

            return data;
        }

        private static void WriteDataToDatabase(IEnumerable<Trade> trades)
        {
            SecureString securePassword = new SecureString();
            foreach (char c in password)
                securePassword.AppendChar(c);
            securePassword.MakeReadOnly();
            SqlCredential credentials = new SqlCredential(userName, securePassword);
            using (SqlConnection connection = new SqlConnection(databaseConnectionString, credentials))
            {
                connection.Open();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    foreach (Trade trade in trades)
                    {
                        SqlCommand command = new SqlCommand(
                            cmdText: "INSERT INTO BitCoinTrades(Instant, Price, Volume, Type) VALUES(@instant, @price, @volume, @type)",
                            connection: connection,
                            transaction: transaction);
                        using (command)
                        {
                            command.Parameters.Add("@instant", SqlDbType.DateTime).Value = trade.Instant;
                            command.Parameters.Add("@price", SqlDbType.Decimal).Value = (decimal)trade.Price;
                            command.Parameters.Add("@volume", SqlDbType.Decimal).Value = (decimal)trade.Volume;
                            command.Parameters.Add("@type", SqlDbType.VarChar, 4).Value = trade.Type;
                            command.CommandType = CommandType.Text;
                            int result = command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                connection.Close();
            }
        }

        private static List<Trade> ParseData(string data)
        {
            return data
                .Replace("[", "")
                .Replace("]", "")
                .Split(',')
                .Select((item, index) => new { Item = item, Index = index })
                .GroupBy(anonymous => anonymous.Index / 4)
                .Select(grouping =>
                {
                    List<double> numbers1 = grouping
                        .Select(item => double.Parse(item.Item, CultureInfo.InvariantCulture))
                        .ToList();
                    double millisecond1 = numbers1[1];
                    double volume1 = numbers1[2];
                    double price1 = numbers1[3];
                    return new Trade()
                    {
                        Instant = GetDateFromMillisecondSince1970(millisecond1),
                        Volume = Math.Abs(volume1),
                        Price = price1,
                        Type = volume1 < 0 ? "SELL" : "BUY",
                    };
                })
                .ToList();
        }

        private static string GetRequestUrl(long first, long last, int limit, bool sort)
        {
            const string url = "https://api.bitfinex.com/v2/trades/tBTCUSD/hist";
            return url + GetParametersUrl(first, last, limit, sort);
        }

        private static NameValueCollection GetNameValueCollection(long first, long last, int limit, bool sort)
        {
            int sortNumber = sort ? 1 : -1;
            NameValueCollection query = new NameValueCollection
            {
                {"start", first.ToString(CultureInfo.InvariantCulture)},
                {"end", last.ToString(CultureInfo.InvariantCulture)},
                {"limit", limit.ToString(CultureInfo.InvariantCulture)},
                {"sort", sortNumber.ToString(CultureInfo.InvariantCulture)}
            };
            return query;
        }

        private static string GetParametersUrl(long first, long last, int limit, bool sort)
        {
            NameValueCollection nameValueCollection = GetNameValueCollection(first, last, limit, sort);
            return ToQueryString(nameValueCollection);
        }

        private static double GetMillisecondsSince1970(DateTime date)
        {
            return (date - date1970).TotalMilliseconds;
        }
        private static DateTime GetDateFromMillisecondSince1970(double milliseconds)
        {
            return date1970.AddMilliseconds(milliseconds);
        }
        public static string ToQueryString(NameValueCollection nvc)
        {
            StringBuilder sb = new StringBuilder();

            foreach (string key in nvc.Keys)
            {
                if (string.IsNullOrEmpty(key)) continue;

                string[] values = nvc.GetValues(key);
                if (values == null) continue;

                foreach (string value in values)
                {
                    sb.Append(sb.Length == 0 ? "?" : "&");
                    sb.AppendFormat("{0}={1}", Uri.EscapeDataString(key), Uri.EscapeDataString(value));
                }
            }

            return sb.ToString();
        }
    }

    public class Trade
    {
        public DateTime Instant { get; set; }
        public double Volume { get; set; }
        public double Price { get; set; }
        public string Type { get; set; }
    }
}
