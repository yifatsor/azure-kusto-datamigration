using System;
using System.Linq;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace ExportImportDatabase
{
    /// <summary>
    /// A sample console app to export a Kusto table into an external table
    /// </summary>
    public class Program
    {
        private static string KustoClusterUri = "https://{cluster-name}.{region}.kusto.windows.net"; 

        static void Main(string[] args)
        {
            // The export is partitioned according to this interval - every export command will cover an ingestion time period according to 
            // this interval. The recommendation is to set this interval so that the export creates files of about 1GB each, and doesn't take over 
            // 10-20 minutes to complete. Adjust this value as appropriate for your database/table. You can run a single export command to test, 
            // adjust accordingly. 
            var exportInterval = TimeSpan.FromMinutes(30);

            // Name of Kusto database and table to export
            var databaseName = "Database";
            var tableName = "T";

            // Name of external table. Must match the schema of the table being exported
            var externalTableName = "ExternalT";
            ExportTableToExternalTable(databaseName, tableName, externalTableName, exportInterval);
        }


        /// <summary>
        /// Export all records from <paramref name="tableName"/> in database <paramref name="databaseName"/> to external table 
        /// <paramref name="externalTableName"/>. Split by intervals of <paramref name="step"/> (of ingestion time)
        /// </summary>
        private static void ExportTableToExternalTable(string databaseName, string tableName, string externalTableName, TimeSpan step)
        {
            // This authenticates based on user credentials. Can replace with AAD app if needed (see KustoConnectionStringBuilder API)
            var connection = new KustoConnectionStringBuilder($"{KustoClusterUri};Fed=true");
            var queryClient = KustoClientFactory.CreateCslQueryProvider(connection);
            var adminClient = KustoClientFactory.CreateCslAdminProvider(connection);
            var minMaxIngestionTime = queryClient.ExecuteQuery<MinMaxDateTime>(databaseName, $"{tableName} | summarize min(ingestion_time()), max(ingestion_time())").Single();

            var start = minMaxIngestionTime.Min;
            var end = minMaxIngestionTime.Max;
            while (start < end)
            {
                DateTime curEnd = start + step;
                var exportCommand = CslCommandGenerator.GenerateExportToExternalTableCommand(externalTableName,
                    query: $"{tableName} | where ingestion_time() >= {CslDateTimeLiteral.AsCslString(start)} and ingestion_time() < {CslDateTimeLiteral.AsCslString(curEnd)}",
                    sizeLimitInBytes: MemoryConstants._1GB,
                    persistDetails: true,
                    isAsync: true);

                var crp = new ClientRequestProperties();
                crp.ClientRequestId = $"ExportApp;{Guid.NewGuid().ToString()}";
                ExtendedConsole.WriteLine(ConsoleColor.Cyan, $"Executing export command from {start.FastToString()} to {curEnd.FastToString()} with request id {crp.ClientRequestId}");
                
                var operationDetails = adminClient.ExecuteAsyncControlCommand(databaseName, exportCommand, TimeSpan.FromMinutes(60), TimeSpan.FromSeconds(1), crp);
                var state = operationDetails.State;
                if (state == "Completed")
                {
                    // check num records exported 
                    var command = $"{CslCommandGenerator.GenerateOperationDetailsShowCommand(operationDetails.OperationId)} | summarize sum(NumRecords)";
                    long exportedRecords = adminClient.ExecuteControlCommand<long>(command).Single();

                    ExtendedConsole.WriteLine(ConsoleColor.Green, $"Operation {operationDetails.OperationId} completed with state {operationDetails.State} and exported {exportedRecords} records");
                }
                else
                {
                    // TODO: retry same scope again (or abort and check root cause for failure). Operation may still be running on server side, so retrying 
                    // without checking status can lead to duplicates
                    ExtendedConsole.WriteLine(ConsoleColor.Red, $"Operation {operationDetails.OperationId} completed with state {operationDetails.State}. Status: {operationDetails.Status}");
                }

                start = curEnd;
            }
        }

        public class MinMaxDateTime
        {
            public DateTime Min;
            public DateTime Max;
        }
    }
}
