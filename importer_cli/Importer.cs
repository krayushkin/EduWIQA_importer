using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Windows.Forms;
using System.IO;

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
        static public Dictionary<int, int> find_all_duplicates(string table, DataTable source_t, OleDbConnection target_con, OleDbCommand select_com, string primary_key)
        {
            Dictionary<int, int> old_new_keys = new Dictionary<int,int>();
            OleDbCommand select_target = select_com.Clone();
            select_target.Connection = target_con;
            OleDbDataReader reader = select_target.ExecuteReader();
            DataTable target_t = new DataTable();
            target_t.Load(reader);
            foreach (DataRow source_row in source_t.Rows)
            {
                foreach (DataRow target_row in target_t.Rows)
                {
                    bool rows_equal = true;
                    foreach (DataColumn col in source_t.Columns)
                    {
                        if (col.ColumnName != primary_key)
                        {
                            if (! source_row[col.ColumnName].Equals(target_row[col.ColumnName]))
                            {
                                rows_equal = false;
                                break;
                            }
                        }
                    }
                    if (rows_equal)
                    {
                        old_new_keys[(int)source_row[primary_key]] = (int)target_row[primary_key];
                        break;
                    }
                }
            }

            return old_new_keys;
        }




        static public void import(TextBox log)
        {
            string source_db = "X:\\ulstu\\equImport\\add\\base3\\base.mdb";
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
            log.AppendText("All done!\n");
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

                Dictionary<int, int> duplicates = find_all_duplicates(table, data_table, target_con, com, infos[table].PrimaryKey);

                foreach (DataRow row in data_table.Rows)
                {
                    foreach (FKey fkey_i in infos[table].FKeys)
                    {
                        int old_key = (int)row[fkey_i.Name];
                        row[fkey_i.Name] = table_old_new_key[fkey_i.Table][old_key];
                    }

                    OleDbCommand target_insert = builder.GetInsertCommand();
                    target_insert.Connection = target_con;

                    foreach (OleDbParameter param in target_insert.Parameters)
                    {
                        param.Value = row[param.SourceColumn];
                    }

                    int old_id = (int)row[infos[table].PrimaryKey];
                    // пробуем найти дубликаты
                    if ( duplicates.ContainsKey( old_id ))
                    {
                        int new_id = duplicates[old_id];
                        table_old_new_key[table][old_id] = new_id;
                    }
                    else
                    {
                        target_insert.ExecuteNonQuery();
                        OleDbCommand new_id_query = new OleDbCommand("SELECT @@IDENTITY", target_con);
                        int new_id = (int)new_id_query.ExecuteScalar();
                        table_old_new_key[table][old_id] = new_id;
                    }

                    
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
