namespace RelationshipTmdlGeneratorUI
{
    partial class Form1
    {
        private System.ComponentModel.IContainer components = null;
        private System.Windows.Forms.Label lblSelectFile;
        private System.Windows.Forms.TextBox txtCsvPath;
        private System.Windows.Forms.Button btnBrowse;
        private System.Windows.Forms.Button btnMigrate;
        private System.Windows.Forms.ProgressBar progressBar;
        private System.Windows.Forms.Label lblStatus;
        private System.Windows.Forms.DataGridView dataGridAudit;
        private System.Windows.Forms.FlowLayoutPanel flowOutputFiles;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        private void InitializeComponent()
        {
            lblSelectFile = new Label();
            txtCsvPath = new TextBox();
            btnBrowse = new Button();
            btnMigrate = new Button();
            progressBar = new ProgressBar();
            lblStatus = new Label();
            dataGridAudit = new DataGridView();
            flowOutputFiles = new FlowLayoutPanel();
            ((System.ComponentModel.ISupportInitialize)dataGridAudit).BeginInit();
            SuspendLayout();
            // 
            // lblSelectFile
            // 
            lblSelectFile.AutoSize = true;
            lblSelectFile.Location = new Point(20, 20);
            lblSelectFile.Name = "lblSelectFile";
            lblSelectFile.Size = new Size(96, 15);
            lblSelectFile.TabIndex = 0;
            lblSelectFile.Text = "Select CSV/Excel:";
            // 
            // txtCsvPath
            // 
            txtCsvPath.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right;
            txtCsvPath.Location = new Point(20, 50);
            txtCsvPath.Name = "txtCsvPath";
            txtCsvPath.ReadOnly = true;
            txtCsvPath.Size = new Size(500, 23);
            txtCsvPath.TabIndex = 1;
            // 
            // btnBrowse
            // 
            btnBrowse.Anchor = AnchorStyles.Top | AnchorStyles.Right;
            btnBrowse.Location = new Point(530, 50);
            btnBrowse.Name = "btnBrowse";
            btnBrowse.Size = new Size(94, 29);
            btnBrowse.TabIndex = 2;
            btnBrowse.Text = "Browse...";
            btnBrowse.Click += btnBrowse_Click;
            // 
            // btnMigrate
            // 
            btnMigrate.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right;
            btnMigrate.BackColor = SystemColors.ActiveBorder;
            btnMigrate.Location = new Point(20, 90);
            btnMigrate.Name = "btnMigrate";
            btnMigrate.Size = new Size(604, 35);
            btnMigrate.TabIndex = 3;
            btnMigrate.Text = "Migrate";
            btnMigrate.UseVisualStyleBackColor = false;
            btnMigrate.Click += btnMigrate_Click;
            // 
            // progressBar
            // 
            progressBar.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right;
            progressBar.Location = new Point(20, 135);
            progressBar.Name = "progressBar";
            progressBar.Size = new Size(604, 25);
            progressBar.TabIndex = 4;
            // 
            // lblStatus
            // 
            lblStatus.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right;
            lblStatus.ForeColor = Color.DarkBlue;
            lblStatus.Location = new Point(20, 170);
            lblStatus.Name = "lblStatus";
            lblStatus.Size = new Size(604, 20);
            lblStatus.TabIndex = 5;
            lblStatus.Text = "Ready.";
            // 
            // dataGridAudit
            // 
            dataGridAudit.AllowUserToAddRows = false;
            dataGridAudit.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
            dataGridAudit.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill;
            dataGridAudit.BackgroundColor = SystemColors.ControlLight;
            dataGridAudit.Location = new Point(20, 200);
            dataGridAudit.Name = "dataGridAudit";
            dataGridAudit.ReadOnly = true;
            dataGridAudit.Size = new Size(604, 106);
            dataGridAudit.TabIndex = 6;
            // 
            // flowOutputFiles
            // 
            flowOutputFiles.Anchor = AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
            flowOutputFiles.AutoScroll = true;
            flowOutputFiles.FlowDirection = FlowDirection.TopDown;
            flowOutputFiles.Location = new Point(20, 326);
            flowOutputFiles.Name = "flowOutputFiles";
            flowOutputFiles.Size = new Size(604, 150);
            flowOutputFiles.TabIndex = 7;
            flowOutputFiles.WrapContents = false;
            // 
            // Form1
            // 
            ClientSize = new Size(784, 630);
            Controls.Add(lblSelectFile);
            Controls.Add(txtCsvPath);
            Controls.Add(btnBrowse);
            Controls.Add(btnMigrate);
            Controls.Add(progressBar);
            Controls.Add(lblStatus);
            Controls.Add(dataGridAudit);
            Controls.Add(flowOutputFiles);
            MinimumSize = new Size(800, 600);
            Name = "Form1";
            Text = "CSV/Excel to TMDL Migration";
            WindowState = FormWindowState.Maximized;
            ((System.ComponentModel.ISupportInitialize)dataGridAudit).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }
    }
}
