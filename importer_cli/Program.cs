using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace importer_cli
{
    class Program
    {
        static int Main(string[] args)
        {
            string help_str =
@"Импорт курсов в из одной базы данных в другую.
Использование:
ИМЯ_ПРОГРАММЫ --source_db=SOURCE_PATH --target_db=TARGET_PATH || [--help]
  --source_db=SOURCE_PATH  путь SOURCE_PATH к базе данных источника курсов
  --target_db=TARGET_PATH  путь TARGET_PATH к целевой базе данных в которую
                           будут скопированы курсы
  --help                   вывод этой справки
";
            if (args.Length == 0)
            {
                System.Console.WriteLine(help_str);
                return 1;
            }

            Dictionary<string, string> param = new Dictionary<string, string>();
            foreach (string com in args)
            {
                char[] sep = new char[] { '=' };
                string[] command_value = com.Split(sep);
                if (command_value.Length == 1)
                    param[command_value[0]] = "";
                else
                    param[command_value[0]] = command_value[1];
            }

            if (param.ContainsKey("--help")) System.Console.WriteLine(help_str);
            else
            {

                string s_db_path;
                string t_db_path;
                try
                {
                    s_db_path = param["--source_db"];
                    t_db_path = param["--target_db"];
                }
                catch (System.Collections.Generic.KeyNotFoundException)
                {
                    Console.WriteLine("Не найдены необходимые параметры для работы! Читайте --help.");
                    return 1;
                }

                try
                {
                    // Делаем всю основную работу здесь
                    Log.InitLog();
                    equImport.Importer.import(s_db_path, t_db_path);

                }
                catch (ApplicationException e)
                {
                    Console.WriteLine(String.Format("При работе программы возникла ошибка: {0}\n Выходим.", e.Message));
                }
                finally
                {
                    Log.EndLog();
                }
            }
            return 0;
        }
    }

    static public class Log
    {
        static public void InitLog()
        {
            Writer = new System.IO.StreamWriter("log.log");
            Writer.WriteLine("----------log started------------");
        }

        static public void EndLog()
        {
            Writer.WriteLine("-----------log ended-------------");
            Writer.Dispose();
        }

        static public System.IO.TextWriter Writer { set; get; }
    }
}
