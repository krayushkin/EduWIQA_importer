using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using importer_cli;

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
            Dictionary<int, int> old_new_keys = new Dictionary<int, int>();
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
                            if (!source_row[col.ColumnName].Equals(target_row[col.ColumnName]))
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




        static public void import(string source_db, string target_db)
        {
            using (OleDbConnection sdb_con = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + source_db),
                   tdb_con = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + target_db))
            {

                try
                {
                    sdb_con.Open();
                    tdb_con.Open();
                }
                catch (OleDbException e)
                {
                    throw new ApplicationException(e.Message);
                }

                Dictionary<string, Dictionary<int, int>> table_old_new_key = new Dictionary<string, Dictionary<int, int>>();
                Dictionary<string, TableInfo> infos = EquSchema.getSchema(sdb_con);

                List<string> already_copied = new List<string>();
                List<string> non_empty = Importer.non_empty_tables(sdb_con, infos);
                foreach (string non_empty_table in non_empty)
                {
                    List<string> deps = new List<string>();
                    EquSchema.traversal_table_dep_from(non_empty_table, infos, (string s) => deps.Add(s));
                    foreach (string table in deps)
                    {
                        if (!already_copied.Contains(table))
                        {
                            Log.Writer.Write("Копирую {0}... \n", table);
                            copy_table(table, infos, sdb_con, tdb_con, table_old_new_key);
                            Log.Writer.Write("OK\n");
                            already_copied.Add(table);
                        }
                    }
                }

            }
            Log.Writer.Write("All done!\n");
        }


        static private void copy_table_with_iowner
           (
           string table,
           Dictionary<string, TableInfo> infos,
           OleDbConnection source_con,
           OleDbConnection target_con,
           Dictionary<string, Dictionary<int, int>> table_old_new_key
           )
        {
            OleDbCommand com = new OleDbCommand("SELECT * from " + table, source_con);
            OleDbDataReader reader = com.ExecuteReader();

            if (reader.HasRows)
            {
                OleDbDataAdapter adapter = new OleDbDataAdapter(com);
                OleDbCommandBuilder builder = new OleDbCommandBuilder(adapter);
                builder.QuotePrefix = "[";
                builder.QuoteSuffix = "]";

                DataTable data_table = new DataTable();
                data_table.Load(reader);
                Log.Writer.WriteLine("Найдено {0} записей.", data_table.Rows.Count.ToString());
                table_old_new_key[table] = new Dictionary<int, int>();

                int insert_counter = 0;
                
                Queue<DataRow> rows_queue = new Queue<DataRow>(data_table.Rows.Cast<DataRow>());
                while (rows_queue.Count > 0)
                {
                    DataRow row = rows_queue.Dequeue();
                    
                    if (((int)row["IOwner"]) != 0)
                    {
                        int old_iowner = (int)row["IOwner"];
                        if (!table_old_new_key[table].ContainsKey(old_iowner))
                        {
                            rows_queue.Enqueue(row);
                            continue;
                        }

                        int new_iowner = table_old_new_key[table][old_iowner];
                        row["IOwner"] = new_iowner;
                        Log.Writer.WriteLine("IOwner set from {0} to {1}", old_iowner, new_iowner);
                    }

                    process_fk_ids(table, infos, table_old_new_key, row);

                    int old_id = (int)row[infos[table].PrimaryKey];

                    insert_row(table, target_con, builder, row);
                    ++insert_counter;

                    int new_id = get_new_id(target_con);
                    table_old_new_key[table][old_id] = new_id;
                    Log.Writer.WriteLine("{0}[{1}] = {2}", table, old_id, new_id);

                }

            }

        }

        private static void insert_row(string table, OleDbConnection target_con, OleDbCommandBuilder builder, DataRow row)
        {
            OleDbCommand target_insert = builder.GetInsertCommand();
            target_insert.Connection = target_con;

            foreach (OleDbParameter param in target_insert.Parameters)
            {
                param.Value = row[param.SourceColumn];
            }

            try
            {
                target_insert.ExecuteNonQuery();
            }
            catch (OleDbException)
            {
                show_db_error_message(table, target_insert);
            }
        }

        private static int get_new_id(OleDbConnection target_con)
        {
            OleDbCommand new_id_query = new OleDbCommand("SELECT @@IDENTITY", target_con);
            int new_id = (int)new_id_query.ExecuteScalar();
            return new_id;
        }

        private static void process_fk_ids(string table, Dictionary<string, TableInfo> infos, Dictionary<string, Dictionary<int, int>> table_old_new_key, DataRow row)
        {
            foreach (FKey fkey_i in infos[table].FKeys)
            {
                int old_key = (int)row[fkey_i.Name];
                row[fkey_i.Name] = table_old_new_key[fkey_i.Table][old_key];
                Log.Writer.WriteLine("{2}: {0} -> {1}", old_key, row[fkey_i.Name], fkey_i.Name);
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
            List<string> tables_with_iowner = new List<string>() { "QAReg", "QAPrec", "PrecGroups"};
            if (tables_with_iowner.Contains(table)) copy_table_with_iowner(table, infos, source_con, target_con, table_old_new_key);
            else copy_table_wo_iowner(table, infos, source_con, target_con, table_old_new_key);
        }

        static private void copy_table_wo_iowner
            (
            string table,
            Dictionary<string, TableInfo> infos,
            OleDbConnection source_con,
            OleDbConnection target_con,
            Dictionary<string, Dictionary<int, int>> table_old_new_key
            )
        {
            OleDbCommand com = new OleDbCommand("SELECT * from " + table, source_con);
            OleDbDataReader reader = com.ExecuteReader();
            
            if (reader.HasRows)
            {
                OleDbDataAdapter adapter = new OleDbDataAdapter(com);
                OleDbCommandBuilder builder = new OleDbCommandBuilder(adapter);
                builder.QuotePrefix = "[";
                builder.QuoteSuffix = "]";

                DataTable data_table = new DataTable();
                data_table.Load(reader);
                Log.Writer.WriteLine("Найдено {0} записей.", data_table.Rows.Count.ToString());
                table_old_new_key[table] = new Dictionary<int, int>();

                Dictionary<int, int> duplicates = find_all_duplicates(table, data_table, target_con, com, infos[table].PrimaryKey);
                Log.Writer.WriteLine("Найдено {0} дублирующих записей", duplicates.Count.ToString());
                int insert_counter = 0;
                foreach (DataRow row in data_table.Rows)
                {
                    process_fk_ids(table, infos, table_old_new_key, row);
                    int old_id = (int)row[infos[table].PrimaryKey];

                    // пробуем найти дубликаты
                    if (duplicates.ContainsKey(old_id))
                    {
                        int new_id = duplicates[old_id];
                        table_old_new_key[table][old_id] = new_id;
                    }
                    else
                    {
                        insert_row(table, target_con, builder, row);
                        ++insert_counter;

                        int new_id = get_new_id(target_con);
                        table_old_new_key[table][old_id] = new_id;
                        Log.Writer.WriteLine("{0}[{1}] = {2}", table, old_id, new_id);
                    }
                }
                Log.Writer.WriteLine("Вставлено {0} записей.", insert_counter.ToString());

            }
            else Log.Writer.WriteLine("Записей не найдено");

        }

        private static void show_db_error_message(string table, OleDbCommand target_insert)
        {
            string param = "";
            foreach (OleDbParameter p in target_insert.Parameters)
            {
                param += String.Format("{0} ({1}): {2}\n", p.SourceColumn, p.OleDbType.ToString(), p.Value.ToString());
            }
            Log.Writer.Write("Ошибка при добавлении записи со следующим содержанием:\n\n{0}\nв таблицу {1}.\n", param, table);
            throw new ApplicationException("Ошибка при добавлении записи.");
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
