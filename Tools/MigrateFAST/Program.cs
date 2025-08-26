using ExcelToTMDL.Models;
using ExcelToTMDL.Services;

namespace ExcelToTMDL
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: ExcelToTMDL.exe <ExcelPath> <OutputFolder>");
                return;
            }

            string excelPath = args[0];
            string outputFolder = args[1];

            if (!File.Exists(excelPath))
            {
                Console.WriteLine($"Excel file not found: {excelPath}");
                return;
            }

            Directory.CreateDirectory(outputFolder);
            string tablesFolder = Path.Combine(outputFolder, "Tables");
            Directory.CreateDirectory(tablesFolder);

            var tables = ExcelConfigReader.ReadTablesFromExcel(excelPath);
            var generator = new TmdlGenerator();

            foreach (var table in tables)
            {
                string tmdlContent = generator.GenerateTmdl(table);
                string tmdlFile = Path.Combine(tablesFolder, $"{table.TableName}.tmdl");
                File.WriteAllText(tmdlFile, tmdlContent);
                Console.WriteLine($"Generated TMDL: {tmdlFile}");
            }

            Console.WriteLine("All TMDL files generated successfully in 'Tables' folder.");
        }
    }
}
