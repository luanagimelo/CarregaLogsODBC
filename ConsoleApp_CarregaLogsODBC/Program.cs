using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Odbc;
using System.Configuration;

namespace ConsoleApp_CarregaLogsODBC
{
    class Program
    {
        internal static string gstrNomeLog =
            ConfigurationManager.AppSettings["PastaLog"] + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
        internal static string gstrNomeLogErros =
            ConfigurationManager.AppSettings["PastaLogErros"] + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
        internal static string strLinhaOrigem;
        internal static string strNomeArquivoOrigem;
        internal static string strPastaOrigem;

        static void Main(string[] args)
        {
            strPastaOrigem = ConfigurationManager.AppSettings["PastaOrigem"];

         // PROCESSAMENTO

         DirectoryInfo di = new DirectoryInfo(strPastaOrigem);

            foreach (FileInfo objArquivo in di.GetFiles("*.txt"))
            {
                strNomeArquivoOrigem = objArquivo.Name;
                StreamWriter sw = File.AppendText(gstrNomeLog);
                Console.WriteLine("Iniciando Processo");
                LerArquivoGravarBanco(strPastaOrigem + strNomeArquivoOrigem, sw);
            }
        }
        // METODOS 
        static void LerArquivoGravarBanco(String parArquivoOrigem, StreamWriter sw)
        {
            String strLinhaDestino;
            String strLog;

            try
            {
                StreamReader sr = new StreamReader(parArquivoOrigem);
                int x = 0;

                do
                {
                    strLinhaOrigem = sr.ReadLine();
                    if (strLinhaOrigem != null)
                    {
                        strLinhaDestino = "INSERT INTO CFTD540D.CFTTB_TRANSMISSOESCFT VALUES (" +
                            "'" + strNomeArquivoOrigem.Substring(0, strNomeArquivoOrigem.IndexOf("_")) + "', " +
                            "'" + strLinhaOrigem.Substring(0, 4).Trim() + "-" +
                                  strLinhaOrigem.Substring(4, 2).Trim() + "-" +
                                  strLinhaOrigem.Substring(6, 3).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(9, 2).Trim() + ":" +
                                  strLinhaOrigem.Substring(11, 2).Trim() + ":" +
                                  strLinhaOrigem.Substring(13, 3).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(16, 6).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(22, 20).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(42, 21).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(63, 21).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(84, 33).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(117, 9).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(126, 9).Trim() + "', ";

                        if (strLinhaOrigem.Length > 235)
                        {
                            strLinhaDestino = strLinhaDestino +
                            "'" + strLinhaOrigem.Substring(135, 101).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(236, 6).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(242).Trim() + "'" +
                            ");";
                        }
                        else
                        {
                            strLinhaDestino = strLinhaDestino +
                            "'" + strLinhaOrigem.Substring(135, 80).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(215, 6).Trim() + "', " +
                            "'" + strLinhaOrigem.Substring(221).Trim() + "'" +
                            ");";
                        }
                        x++;

                        strLog = " | " +
                                "Linha do registro: " + x + " | " +
                                parArquivoOrigem + " | " +
                                strLinhaDestino;
                        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + strLog);

                        Gravar(strLinhaDestino, sw);
                        Console.WriteLine("Linha do registro: " + x + " ID: " + strLinhaOrigem.Substring(117, 9).TrimEnd());
                    }
                } while (strLinhaOrigem != null);

                sr.Close();
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + ex);
                
                StreamWriter swEx = File.AppendText(gstrNomeLogErros);
                swEx.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + " | " + "Arquivo original: " + strNomeArquivoOrigem + " | " +
                    "Linha geradora do erro: " + strLinhaOrigem + "\n" + "Erro: " + ex);
                
                Console.WriteLine("Exception: " + ex.Message);

                swEx.Close();
            }
            finally
            {
                Console.WriteLine("Final da Leitura de origem e gravação no destino.");
                sw.Close();
            }
        }

        static private String MontarStringConexao(StreamWriter sw)
        {
            string strConexao = "";
            try
            {
                strConexao = ConfigurationManager.ConnectionStrings["ConsoleApp_CarregaLogsODBC.Properties.Settings.ConnectionString"].ConnectionString;
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + ex);

                StreamWriter swEx = File.AppendText(gstrNomeLogErros);
                swEx.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + " | " + "Arquivo original: " + strNomeArquivoOrigem + " | " +
                    "Linha geradora do erro: " + strLinhaOrigem + "\n" + "Erro: " + ex);

                Console.WriteLine("Exception: " + ex.Message);

                swEx.Close();
            }
            return strConexao;
        }

        static void Gravar(String strLinhaDestino, StreamWriter sw)
        {
            try
            {
                OdbcConnection conn = new OdbcConnection(MontarStringConexao(sw));
                conn.Open();

                OdbcCommand cmd = new OdbcCommand(strLinhaDestino, conn);
                cmd.ExecuteNonQuery();

                conn.Close();
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + ex);

                StreamWriter swEx = File.AppendText(gstrNomeLogErros);
                swEx.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff ") + " | " + "Arquivo original: " + strNomeArquivoOrigem + " | " +
                    "Linha geradora do erro: " + strLinhaOrigem + "\n" + "Erro: " + ex);

                Console.WriteLine("Exception: " + ex.Message);

                swEx.Close();
            }
        }
    }
}
