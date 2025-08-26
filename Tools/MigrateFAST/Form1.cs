using System;
using System.IO;
using System.Windows.Forms;

namespace RelationshipTmdlGeneratorUI
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        // Browse button click handler
        private void btnBrowse_Click(object sender, EventArgs e)
        {
            using OpenFileDialog ofd = new OpenFileDialog();
            ofd.Filter = "Excel Files|*.xlsx;*.xls|CSV Files|*.csv";
            ofd.Title = "Select Excel or CSV File";

            if (ofd.ShowDialog() == DialogResult.OK)
            {
                txtExcelPath.Text = ofd.FileName;
                lblStatus.Text = "";
            }
        }

        // Generate button click handler
        private void btnGenerate_Click(object sender, EventArgs e)
        {
            string excelPath = txtExcelPath.Text;
            if (!File.Exists(excelPath))
            {
                lblStatus.Text = "❌ Excel file not found!";
                return;
            }

            try
            {
                var relationships = TmdlService.ReadRelationships(excelPath);

                string desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                string outputPath = Path.Combine(desktopPath, "relationships.tmdl");

                TmdlService.WriteTmdl(relationships, outputPath);

                lblStatus.Text = $"✅ Generated {relationships.Count} relationships at Desktop!";
            }
            catch (Exception ex)
            {
                lblStatus.Text = $"❌ Error: {ex.Message}";
            }
        }
    }
}
