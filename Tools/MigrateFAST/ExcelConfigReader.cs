using ClosedXML.Excel;
using System.Data;
using ExcelToTMDL.Models;

namespace ExcelToTMDL.Services
{
    public static class ExcelConfigReader
    {
        public static List<TableConfig> ReadTablesFromExcel(string excelPath)
        {
            var tables = new Dictionary<string, TableConfig>();

            using (var workbook = new XLWorkbook(excelPath))
            {
                var ws = workbook.Worksheets.First();
                var table = new DataTable();

                // Load Excel headers
                bool firstRow = true;
                foreach (var row in ws.RowsUsed())
                {
                    if (firstRow)
                    {
                        foreach (var cell in row.Cells())
                            table.Columns.Add(cell.Value.ToString());
                        firstRow = false;
                    }
                    else
                    {
                        var newRow = table.NewRow();
                        int i = 0;
                        foreach (var cell in row.Cells(1, table.Columns.Count))
                        {
                            newRow[i++] = cell.Value.IsBlank ? "" : cell.Value.ToString();
                        }
                        table.Rows.Add(newRow);
                    }
                }

                // Build TableConfigs
                foreach (DataRow row in table.Rows)
                {
                    string tableName = row["TableName"]?.ToString() ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(tableName))
                        continue;

                    if (!tables.ContainsKey(tableName))
                    {
                        tables[tableName] = new TableConfig
                        {
                            TableName = tableName,
                            DatabaseName = row["DatabaseName"]?.ToString() ?? string.Empty,
                            ServerName = row["ServerName"]?.ToString() ?? string.Empty,
                            Mode = row["Mode"]?.ToString() ?? "directQuery"
                        };
                    }

                    tables[tableName].Columns.Add(new ColumnConfig
                    {
                        ColumnName = row["ColumnName"]?.ToString() ?? string.Empty,
                        DataType = row["ColumnDatatype"]?.ToString() ?? string.Empty,
                        SourceColumn = row["SourceColumnName"]?.ToString() ?? string.Empty
                    });
                }
            }

            return tables.Values.ToList();
        }
    }
}
