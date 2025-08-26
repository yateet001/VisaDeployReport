namespace ExcelToTMDL.Models
{
    public class TableConfig
    {
        public string TableName { get; set; } = string.Empty;
        public string DatabaseName { get; set; } = string.Empty;
        public string ServerName { get; set; } = string.Empty;
        public string Mode { get; set; } = "directQuery"; // default
        public List<ColumnConfig> Columns { get; set; } = new List<ColumnConfig>();
    }
}
