using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Data.OleDb;


namespace equImport
{
   
    public partial class Form1 : Form
    {

        public Form1()
        {
            InitializeComponent();


            string source_db = "X:\\ulstu\\equImport\\add\\base1\\base.mdb";
            string target_db = "X:\\ulstu\\equImport\\Base\\base.mdb";
            using (OleDbConnection connection = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + source_db))
            {
                    Dictionary<string, TableInfo> infos_dict = new Dictionary<string, TableInfo>();
                    DataTable dt;
                    connection.Open();
                    
                    dt = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                    dataGridView1.DataSource = dt;
                   

            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            Importer.test_copy_table(textBox1);
        }
    }

    

}
