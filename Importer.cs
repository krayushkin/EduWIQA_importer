﻿using System;
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

        public static Dictionary<string, TableInfo> getSchema(OleDbConnection connection)
        {
            Dictionary<string, TableInfo> infos_dict = new Dictionary<string, TableInfo>();
            DataTable dt;
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

            return infos_dict;
        }
    }

    

    public static class Importer
    {
        /// <summary>
        /// Ищет дубликат в записи с параметрами в param
        /// </summary>
        /// <param name="table">Целевая таблица</param>
        /// <param name="con"></param>
        /// <param name="param"></param>
        /// <param name="primary_key"></param>
        /// <param name="id">Главный ключ записи дубликата</param>
        /// <returns>
        /// id - есть ли дубликат
        /// </returns>
        static public bool find(string table, OleDbConnection con, OleDbParameterCollection param, string primary_key, out int id)
        {
            string query_start = String.Format("SELECT * FROM [{0}] WHERE ", table);
            string query_end = "";
            
            
            for (int i = 0; i < param.Count; i++)
            {
                if (i == param.Count - 1)
                {
                    query_end += String.Format("[{0}] = ?", param[i].SourceColumn);
                }
                else
                {
                    query_end += String.Format("[{0}] = ?, ", param[i].SourceColumn);
                }
            }

            OleDbCommand com = new OleDbCommand(query_start+query_end, con);
            foreach (OleDbParameter p in param)
            {
                com.Parameters.Add(p);
            }

           bool result;
           OleDbDataReader reader = com.ExecuteReader();
           DataTable t = new DataTable();
           t.Load(reader);
           if (t.Rows.Count > 1) throw new ApplicationException("more then 1 row");
           if (t.Rows.Count == 1) result = true;
           else result = false;
           if (result)
           {
               id = (int)t.Rows[0][primary_key];
           }
           else
           {
               id = 0;
           }

           return result;
        }




        static public void test_copy_table(TextBox log)
        {
            string source_db = "X:\\ulstu\\equImport\\add\\base1\\base.mdb";
            string target_db = "X:\\ulstu\\equImport\\base\\base.mdb";
            using (OleDbConnection sdb_con = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source="+source_db))
            {
                using (OleDbConnection tdb_con = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source="+target_db))
                {
                    sdb_con.Open();
                    tdb_con.Open();

                    Dictionary<string, Dictionary<int, int>> table_old_new_key = new Dictionary<string, Dictionary<int, int>>();
                    Dictionary<string, TableInfo> infos = EquSchema.getSchema(sdb_con);

                    List<string> already_copied = new List<string>();
                    List<string> non_empty = Importer.non_empty_tables(sdb_con, infos);
                    foreach (string non_empty_table in non_empty)
                    {
                        List<string> deps = new List<string>();
                        EquSchema.traversal_table_dep_from(non_empty_table, infos, (string s)=> deps.Add(s));
                        foreach (string table in deps)
                        {
                            if (!already_copied.Contains(table))
                            {
                                log.AppendText(String.Format("copy {0}\n", table));
                                copy_table(table, infos, sdb_con, tdb_con, table_old_new_key);
                                already_copied.Add(table);
                            }
                        }
                    }

                    
                }
            }
        }

        static public void copy_table
            (
            string table,
            Dictionary<string, TableInfo> infos,
            OleDbConnection source_con,
            OleDbConnection target_con,
            Dictionary<string, Dictionary<int, int>> table_old_new_key
            )
        {
            OleDbCommand com = new OleDbCommand("SELECT * from "+ table, source_con);
            OleDbDataReader reader = com.ExecuteReader();
            if (reader.HasRows)
            {
                OleDbDataAdapter adapter = new OleDbDataAdapter(com);
                OleDbCommandBuilder builder = new OleDbCommandBuilder(adapter);
                builder.QuotePrefix = "[";
                builder.QuoteSuffix = "]";

                DataTable data_table = new DataTable();
                data_table.Load(reader);

                table_old_new_key[table] = new Dictionary<int, int>();
                foreach (DataRow row in data_table.Rows)
                {
                    foreach (FKey fkey_i in infos[table].FKeys)
                    {
                        int old_key = (int)row[fkey_i.Name];
                        row[fkey_i.Name] = table_old_new_key[fkey_i.Table][old_key];
                    }

                    if (table == "Users")
                    {
                        row["SNick"] = "imp_" + ((string)row["SNick"]);
                    }
                    if (table == "Groups")
                    {
                        row["SName"] = "imp_" + (string)row["SName"];
                    }

                    List<string> no_copy = new List<string>();
                    no_copy.Add("QAStatus");
                    no_copy.Add("QATypes");
                    no_copy.Add("OntologyLinkTypes");
                    

                    
                    if ( no_copy.Contains(table))
                    {
                        int old = (int)row[infos[table].PrimaryKey];
                        table_old_new_key[table][old] = old;
                        continue;
                    }
               

                    OleDbCommand target_insert = builder.GetInsertCommand();
                    target_insert.Connection = target_con;

                    foreach (OleDbParameter param in target_insert.Parameters)
                    {
                        param.Value = row[param.SourceColumn];
                    }

                    target_insert.ExecuteNonQuery();
                    OleDbCommand new_id_query = new OleDbCommand("SELECT @@IDENTITY", target_con);
                    int new_id = (int)new_id_query.ExecuteScalar();
                    int old_id = (int)row[infos[table].PrimaryKey];
                    table_old_new_key[table][old_id] = new_id;
                }
                

            }

        }

        static public List<string> non_empty_tables(OleDbConnection connection, Dictionary<string, TableInfo> infos)
        {
            List<string> table_with_rows = new List<string>();
            foreach (string key in infos.Keys)
            {
                OleDbCommand com = new OleDbCommand("SELECT * from " + key, connection);
                OleDbDataReader reader = com.ExecuteReader();
                if (reader.HasRows) table_with_rows.Add(key);
            }
            return table_with_rows;
        }

    }
}
