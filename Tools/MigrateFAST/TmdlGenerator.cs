using ExcelToTMDL.Models;

namespace ExcelToTMDL.Services
{
    public class TmdlGenerator
    {
        public string GenerateTmdl(TableConfig table)
        {
            var tmdlLines = new List<string>
            {
                "createOrReplace",
                $"\ttable {table.TableName}",
                $"\t\tlineageTag: {Guid.NewGuid()}",
                ""
            };

            foreach (var column in table.Columns)
            {
                if (string.IsNullOrWhiteSpace(column.ColumnName))
                    continue;

                tmdlLines.Add($"\t\tcolumn {column.ColumnName}");
                tmdlLines.Add($"\t\t\tdataType: {column.DataType}");
                tmdlLines.Add($"\t\t\tlineageTag: {Guid.NewGuid()}");
                tmdlLines.Add($"\t\t\tsourceColumn: {column.SourceColumn}");
                tmdlLines.Add($"\t\t\tannotation SummarizationSetBy = Automatic");
                tmdlLines.Add("");
            }

            // Partition info
            tmdlLines.Add($"\t\tpartition {table.TableName} = m");
            tmdlLines.Add($"\t\t\tmode: {table.Mode}");
            tmdlLines.Add("\t\t\tsource =");
            tmdlLines.Add("\t\t\t\tlet");
            tmdlLines.Add($"\t\t\t\t\tSource = Sql.Database(\"{table.ServerName}\", \"{table.DatabaseName}\"),");
            tmdlLines.Add($"\t\t\t\t\tdbo_{table.TableName} = Source{{[Schema=\"dbo\",Item=\"{table.TableName}\"]}}[Data]");
            tmdlLines.Add("\t\t\t\tin");
            tmdlLines.Add($"\t\t\t\t\tdbo_{table.TableName}");
            tmdlLines.Add("");
            tmdlLines.Add("\t\tannotation PBI_ResultType = Table");

            return string.Join(Environment.NewLine, tmdlLines);
        }
    }
}
