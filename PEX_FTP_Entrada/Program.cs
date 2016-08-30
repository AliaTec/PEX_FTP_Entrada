using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;
using System.Configuration;
using System.IO;
using System.Net;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;

namespace PEX_FTP_Entrada
{
    class Program
    {
        static void Main(string[] args)
        {
            //Se obtienen valores de configuración
            string ConString = ConfigurationManager.ConnectionStrings["SQLConnection"].ConnectionString;
            string FTPHost = ConfigurationManager.AppSettings["FTPHost"];
            string FTPUser = ConfigurationManager.AppSettings["FTPUser"];
            string FTPPass = ConfigurationManager.AppSettings["FTPPass"];
            int FTPPort = Int32.Parse(ConfigurationManager.AppSettings["FTPPort"]);
            string FTPDirectory = ConfigurationManager.AppSettings["FTPDirectory"];
            string Path = ConfigurationManager.AppSettings["PathFile"];

            GetFiles(FTPHost, FTPUser, FTPPass, FTPPort,Path,FTPDirectory,ConString);
            //Console.ReadLine();
        }

        public static void GetFiles(string host, string user, string pass, int port,string path,string directory,string constr)
        {
            
            int countFiles = 0;
            try
            {
                //Se define el objeto de conexión
                SftpClient sftpClient = new SftpClient(host,port, user, pass); 
                sftpClient.Connect();
                //Se declara lista de archivos en arreglo
                List<SftpFile> fileList = sftpClient.ListDirectory(directory).ToList();
                //Se obtiene cuantos archivos hay
                countFiles = fileList.Count();

                if (fileList != null && countFiles > 0)
                {
                    for (int i = 0; i < 1; i++)//fileList.Count(); i++)
                    {
                        string destinationFile = path + fileList[i].Name;  
                        using (var stream = new FileStream(destinationFile, FileMode.Create))
                        {
                            sftpClient.DownloadFile(fileList[i].FullName, stream); //Se descargan los archivos
                            stream.Close();
                            ProcessFile(constr, destinationFile,fileList[i].Name);
                            sftpClient.DeleteFile(directory + "/" + fileList[i].Name); //Borra el archivo del EFT
                        }
                    }
                }
                sftpClient.Disconnect();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static void ProcessFile (string constr,string File,string FileName)
        {
            int r=0;
            SqlConnection con = new SqlConnection(constr);
            string filepath = File;
            StreamReader sr = new StreamReader(filepath);
            //string line ="\"TaskBlockID\"," + sr.ReadLine();
            
            DataTable dt = new DataTable();
            DataRow row;

            string taskBlockID=DateTime.Now.ToString("yyyyMMddHHmmss");        

            dt.Columns.Add(new DataColumn());
            dt.Columns.Add(new DataColumn());
            dt.Columns.Add(new DataColumn());
            dt.Columns.Add(new DataColumn());
            dt.Columns.Add(new DataColumn());

            while (!sr.EndOfStream)
            {
                string line = "131"+"?"+taskBlockID+"?" + sr.ReadLine() + "?1?NULL";
                if (r==0)
                {
                    line = line.Replace(".", "");
                }
                line = line.Replace("\"", "");
                line = line.Replace(",", "|");
                string[] value = line.Split('?');
                //value = line.Split('?');
                if (value.Length == dt.Columns.Count)
                {
                    row = dt.NewRow();
                    row.ItemArray = value;
                    dt.Rows.Add(row);
                }
                r = 1;
            }
            SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
            bc.DestinationTableName = "TaskRecord";
            bc.BatchSize = dt.Rows.Count;
            con.Open();
            bc.WriteToServer(dt);
            bc.Close();
            con.Close();

            using (SqlCommand cmd = new SqlCommand("sp_TaskBlock", con))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@idTask", SqlDbType.VarChar).Value = 131;
                cmd.Parameters.Add("@taskBlockId", SqlDbType.VarChar).Value = taskBlockID;
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }

            using (SqlCommand cmd = new SqlCommand("spa_PEX_Files", con))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Nombre", SqlDbType.VarChar).Value =FileName;
                cmd.Parameters.Add("@taskBlockID", SqlDbType.VarChar).Value = taskBlockID;
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }

            using (SqlCommand cmd = new SqlCommand("sp_ConfirmationLog_PEX", con))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@folioDesde", SqlDbType.VarChar).Value = 1;
                cmd.Parameters.Add("@folioHasta", SqlDbType.VarChar).Value = taskBlockID;//FileName.Substring(FileName.Length - 6, 3);
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }


        }

       

    }
}
