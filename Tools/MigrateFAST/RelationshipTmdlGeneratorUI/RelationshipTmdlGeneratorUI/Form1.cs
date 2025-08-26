using System;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace RelationshipTmdlGeneratorUI
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            this.WindowState = FormWindowState.Maximized;
        }

        private void btnBrowse_Click(object sender, EventArgs e)
        {
            using OpenFileDialog ofd = new OpenFileDialog
            {
                Filter = "CSV Files|*.csv|Excel Files|*.xlsx;*.xls",
                Title = "Select CSV or Excel File"
            };

            if (ofd.ShowDialog() == DialogResult.OK)
            {
                txtCsvPath.Text = ofd.FileName;
                lblStatus.Text = "";
            }
        }

        private async void btnMigrate_Click(object sender, EventArgs e)
        {
            string csvPath = txtCsvPath.Text;
            if (!File.Exists(csvPath))
            {
                lblStatus.Text = "❌ File not found!";
                return;
            }

            try
            {
                progressBar.Value = 0;
                lblStatus.Text = "Processing...";

                await Task.Run(() =>
                {
                    var relationships = TmdlService.ReadRelationships(csvPath);

                    string desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                    string outputFolder = Path.Combine(desktopPath, "TMDL_Output");
                    Directory.CreateDirectory(outputFolder);

                    string tmdlPath = Path.Combine(outputFolder, "relationships.tmdl");
                    TmdlService.WriteTmdl(relationships, tmdlPath);

                    string auditPath = Path.Combine(outputFolder, "audit.csv");
                    TmdlService.WriteAudit(relationships, auditPath);

                    this.Invoke(() =>
                    {
                        progressBar.Value = 100;
                        lblStatus.Text = $"✅ Migration completed! Output in {outputFolder}";

                        // Clear old links
                        flowOutputFiles.Controls.Clear();

                        foreach (var filePath in new[] { tmdlPath, auditPath })
                        {
                            var panel = new Panel
                            {
                                Width = flowOutputFiles.Width - 25,
                                Height = 40,
                                BorderStyle = BorderStyle.FixedSingle,
                                Margin = new Padding(5),
                                Cursor = Cursors.Hand,
                                BackColor = Color.LightSkyBlue
                            };

                            var lbl = new Label
                            {
                                Text = Path.GetFileName(filePath),
                                AutoSize = false,
                                Dock = DockStyle.Fill,
                                TextAlign = ContentAlignment.MiddleLeft,
                                Padding = new Padding(10, 0, 0, 0),
                                Font = new Font("Segoe UI", 10, FontStyle.Bold)
                            };

                            panel.Controls.Add(lbl);
                            panel.Click += (s, e) => OpenFile(filePath);
                            lbl.Click += (s, e) => OpenFile(filePath);

                            flowOutputFiles.Controls.Add(panel);
                        }

                        dataGridAudit.DataSource = TmdlService.GetAuditDataTable(relationships);
                    });
                });
            }
            catch (Exception ex)
            {
                lblStatus.Text = $"❌ Error: {ex.Message}";
            }
        }

        private void OpenFile(string path)
        {
            try
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = path,
                    UseShellExecute = true
                });
            }
            catch
            {
                MessageBox.Show("Cannot open file: " + path);
            }
        }
    }
}
