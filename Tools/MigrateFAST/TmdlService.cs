using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ClosedXML.Excel;

namespace RelationshipTmdlGeneratorUI
{
    class TmdlService
    {
        public static List<Relationship> ReadRelationships(string excelPath)
        {
            var relationships = new List<Relationship>();

            using var workbook = new XLWorkbook(excelPath);
            var worksheet = workbook.Worksheet(1);
            var rows = worksheet.RangeUsed().RowsUsed();

            foreach (var row in rows.Skip(1))
            {
                string fromTable = row.Cell(1).GetString().Trim();
                string fromCol = row.Cell(2).GetString().Trim();
                string toTable = row.Cell(3).GetString().Trim();
                string toCol = row.Cell(4).GetString().Trim();
                string card = row.Cell(5).GetString().Trim();
                string cross = row.Cell(6).GetString().Trim();

                if (string.IsNullOrWhiteSpace(fromTable) || string.IsNullOrWhiteSpace(toTable))
                    continue;

                relationships.Add(new Relationship
                {
                    FromTable = fromTable,
                    FromColumn = fromCol,
                    ToTable = toTable,
                    ToColumn = toCol,
                    Cardinality = string.IsNullOrWhiteSpace(card) ? "ManyToOne" : card,
                    CrossFilteringBehavior = string.IsNullOrWhiteSpace(cross) ? "Single" : cross
                });
            }

            return relationships;
        }

        public static void WriteTmdl(List<Relationship> rels, string outPath)
        {
            using var writer = new StreamWriter(outPath);

            foreach (var r in rels)
            {
                string name = $"{r.FromTable}_{r.ToTable}";
                writer.WriteLine("createOrReplace");
                writer.WriteLine($"\trelationship {name}");
                writer.WriteLine($"\t\tfromColumn: {r.FromTable}[{r.FromColumn}]");
                writer.WriteLine($"\t\ttoColumn:   {r.ToTable}[{r.ToColumn}]");
                writer.WriteLine($"\t\tcardinality: {r.Cardinality}");
                writer.WriteLine($"\t\tcrossFilteringBehavior: {r.CrossFilteringBehavior}");
                writer.WriteLine($"\t\tlineageTag: {Guid.NewGuid()}");
                writer.WriteLine();
            }
        }
    }
}
