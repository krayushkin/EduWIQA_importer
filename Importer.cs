using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Windows.Forms;

namespace equImport
{

    public class FKey
    {
        public string Name
        {
            set;
            get;
        }

        public string Table
        {
            set;
            get;
        }

        public string Column
        {
            set;
            get;
        }
    }

    public class TableInfo
    {

        public TableInfo()
        {
            FKeys = new List<FKey>();
        }

        public string TableName
        {
            set;
            get;
        }

        public string PrimaryKey
        {
            set;
            get;
        }

        public List<FKey> FKeys
        {
            set;
            get;
        }
    }

    public static class EquSchema
    {


        public static void traversal_table_dep_from(string table, Dictionary<string, TableInfo> infos_dict, Action<string> act)
        {
            foreach (FKey fk in infos_dict[table].FKeys)
            {
                traversal_table_dep_from(fk.Table, infos_dict, act);
            }
            act(table);
        }

        public static void traversal_they_dep_from_table(string table, Dictionary<string, TableInfo> infos_dict, Action<string> act)
        {

            var t = infos_dict.Where((KeyValuePair<string, TableInfo> kv) => true ? kv.Value.FKeys.Find((FKey k) => k.Table == table) != null : false);
            foreach (KeyValuePair<string, TableInfo> ti in t)
            {
                traversal_they_dep_from_table(ti.Value.TableName, infos_dict, act);
            }
            act(table);
        }

        public static void getSchema()
        {
            string connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=X:\\ulstu\\equImport\\base\\base.mdb";

            Dictionary<string, TableInfo> infos_dict = new Dictionary<string, TableInfo>();

            DataTable dt;

            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                dt = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                foreach (DataRow row in dt.Rows)
                {

                    if (row["TABLE_TYPE"] as string == "TABLE")
                    {
                        TableInfo info = new TableInfo { TableName = row["TABLE_NAME"] as string };
                        infos_dict[info.TableName] = info;
                    }
                }

                int i = 0;
                dt = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, null);
                foreach (DataRow row in dt.Rows)
                {
                    string table_name = row["TABLE_NAME"] as string;
                    if (infos_dict.ContainsKey(table_name))
                    {
                        i++;
                        infos_dict[table_name].PrimaryKey = row["COLUMN_NAME"] as string;
                    }
                }

                if (i != infos_dict.Count) throw new System.ApplicationException();

                dt = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys, null);
                foreach (DataRow row in dt.Rows)
                {
                    string fk_table_name = row["FK_TABLE_NAME"] as string;
                    if (infos_dict.ContainsKey(fk_table_name))
                    {
                        FKey key = new FKey
                        {
                            Name = row["FK_COLUMN_NAME"] as string,
                            Table = row["PK_TABLE_NAME"] as string,
                            Column = row["PK_COLUMN_NAME"] as string
                        };
                        infos_dict[fk_table_name].FKeys.Add(key);
                    }
                }

                infos_dict["Users"] = infos_dict["USERS"];
                infos_dict.Remove("USERS");

                string l = "";
                traversal_table_dep_from("Demonstrations", infos_dict, (string t) => l += t + "; ");
                MessageBox.Show(l);

                l = "";
                traversal_they_dep_from_table("Demonstrations", infos_dict, (string t) => l += t + "; ");
                MessageBox.Show(l);
            }
        }
    }


    public static class Importer
    {

    }
}
