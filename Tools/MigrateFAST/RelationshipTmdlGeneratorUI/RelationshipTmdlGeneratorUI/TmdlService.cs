using System;

using System.Collections.Generic;

using System.Data;

using System.IO;

using System.Linq;

using ClosedXML.Excel;

namespace RelationshipTmdlGeneratorUI

{

    class TmdlService

    {

        public static List<Relationship> ReadRelationships(string path)

        {

            var relationships = new List<Relationship>();

            if (path.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))

            {

                var lines = File.ReadAllLines(path);

                if (lines.Length < 2) return relationships;

                // Normalize headers

                var headers = lines[0].Split(',').Select(h => h.Trim()).ToArray();

                int fromTableIdx = Array.FindIndex(headers, h => h.Equals("FromTable", StringComparison.OrdinalIgnoreCase));

                int fromColumnIdx = Array.FindIndex(headers, h => h.Equals("FromColumn", StringComparison.OrdinalIgnoreCase));

                int toTableIdx = Array.FindIndex(headers, h => h.Equals("ToTable", StringComparison.OrdinalIgnoreCase));

                int toColumnIdx = Array.FindIndex(headers, h => h.Equals("ToColumn", StringComparison.OrdinalIgnoreCase));

                int cardinalityIdx = Array.FindIndex(headers, h => h.Equals("Cardinality", StringComparison.OrdinalIgnoreCase));

                for (int i = 1; i < lines.Length; i++)

                {

                    var parts = lines[i].Split(',');

                    if (parts.Length <= Math.Max(fromColumnIdx, toColumnIdx)) continue;

                    relationships.Add(new Relationship

                    {

                        FromTable = parts[fromTableIdx].Trim(),

                        FromColumn = parts[fromColumnIdx].Trim(),

                        ToTable = parts[toTableIdx].Trim(),

                        ToColumn = parts[toColumnIdx].Trim(),

                        Cardinality = cardinalityIdx >= 0 ? parts[cardinalityIdx].Trim() : ""

                    });

                }

            }

            else // Excel

            {

                using var workbook = new XLWorkbook(path);

                var ws = workbook.Worksheet(1);

                // Read headers first

                var headerRow = ws.FirstRowUsed();

                var headers = headerRow.Cells().Select(c => c.GetString().Trim()).ToArray();

                int fromTableIdx = Array.FindIndex(headers, h => h.Equals("FromTable", StringComparison.OrdinalIgnoreCase)) + 1;

                int fromColumnIdx = Array.FindIndex(headers, h => h.Equals("FromColumn", StringComparison.OrdinalIgnoreCase)) + 1;

                int toTableIdx = Array.FindIndex(headers, h => h.Equals("ToTable", StringComparison.OrdinalIgnoreCase)) + 1;

                int toColumnIdx = Array.FindIndex(headers, h => h.Equals("ToColumn", StringComparison.OrdinalIgnoreCase)) + 1;

                int cardinalityIdx = Array.FindIndex(headers, h => h.Equals("Cardinality", StringComparison.OrdinalIgnoreCase)) + 1;

                foreach (var row in ws.RowsUsed().Skip(1))

                {

                    relationships.Add(new Relationship

                    {

                        FromTable = row.Cell(fromTableIdx).GetString(),

                        FromColumn = row.Cell(fromColumnIdx).GetString(),

                        ToTable = row.Cell(toTableIdx).GetString(),

                        ToColumn = row.Cell(toColumnIdx).GetString(),

                        Cardinality = cardinalityIdx > 0 ? row.Cell(cardinalityIdx).GetString() : ""

                    });

                }

            }

            return relationships;

        }

        public static void WriteTmdl(List<Relationship> relationships, string path)

        {

            using var sw = new StreamWriter(path);

            if (relationships == null || relationships.Count == 0)

            {

                sw.WriteLine("no active relationships");

                return;

            }

            sw.WriteLine("createOrReplace");

            sw.WriteLine();

            foreach (var rel in relationships)

            {

                string fromExpr = $"{rel.FromTable}.{rel.FromColumn}";

                string toExpr = $"{rel.ToTable}.{rel.ToColumn}";

                string guid = Guid.NewGuid().ToString();

                sw.WriteLine($"    relationship {guid}");

                sw.WriteLine($"        fromColumn: {fromExpr}");

                sw.WriteLine($"        toColumn: {toExpr}");

                sw.WriteLine();

            }

        }

        public static void WriteAudit(List<Relationship> relationships, string path)

        {

            using var sw = new StreamWriter(path);

            sw.WriteLine("FromTable,FromColumn,ToTable,ToColumn,Cardinality");

            foreach (var rel in relationships)

            {

                sw.WriteLine($"{rel.FromTable},{rel.FromColumn},{rel.ToTable},{rel.ToColumn},{rel.Cardinality}");

            }

        }

        public static DataTable GetAuditDataTable(List<Relationship> relationships)

        {

            var dt = new DataTable();

            dt.Columns.Add("FromTable");

            dt.Columns.Add("FromColumn");

            dt.Columns.Add("ToTable");

            dt.Columns.Add("ToColumn");

            dt.Columns.Add("Cardinality");

            foreach (var rel in relationships)

            {

                dt.Rows.Add(rel.FromTable, rel.FromColumn, rel.ToTable, rel.ToColumn, rel.Cardinality);

            }

            return dt;

        }

    }

}

