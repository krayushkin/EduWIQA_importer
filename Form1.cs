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
            Importer.test_copy_table();


            string connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=X:\\ulstu\\equImport\\base\\base.mdb";
            DataTable t = new DataTable();
            DataTable t2;

            List<string> tables = new List<string>();

            DataSet set = new DataSet();
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();

                OleDbCommand com = new OleDbCommand("SELECT * from Hints", connection);
                OleDbDataAdapter adapter = new OleDbDataAdapter();
                adapter.SelectCommand = com;
                OleDbCommandBuilder builder = new OleDbCommandBuilder(adapter);
                
                adapter.Fill(set);
                t2 = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, null);
                

                // The connection is automatically closed at 
                // the end of the Using block.
            }

            dataGridView1.DataSource = set.Tables[0];
            dataGridView2.DataSource = t2;
            


        }
    }

    

}
