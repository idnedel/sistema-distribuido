using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace DistributedStorageSystem
{
    public class Client
    {
        private readonly int port;

        public Client(int port)
        {
            this.port = port;
        }

        public void Upload(string filePath)
        {
            Send((writer, reader) =>
            {
                string filename = Path.GetFileName(filePath);
                byte[] data = File.ReadAllBytes(filePath);

                writer.Write("UPLOAD");
                writer.Write(filename);
                writer.Write(data.Length);
                writer.Write(data);

                string response = reader.ReadString();
                Console.WriteLine($"UPLOAD -> {response}");
            });
        }

        public void List()
        {
            Send((writer, reader) =>
            {
                writer.Write("LIST_GLOBAL");

                string resp = reader.ReadString();
                if (resp != "OK")
                {
                    Console.WriteLine($"LIST -> {resp}");
                    return;
                }

                int count = reader.ReadInt32();
                SortedSet<string> files = new SortedSet<string>();
                for (int i = 0; i < count; i++)
                {
                    files.Add(reader.ReadString());
                }

                Console.WriteLine("Arquivos no sistema:");
                foreach (string file in files)
                {
                    Console.WriteLine($" - {file}");
                }
            });
        }

        public void Download(string filename, string outputPath)
        {
            Send((writer, reader) =>
            {
                writer.Write("DOWNLOAD");
                writer.Write(filename);

                string resp = reader.ReadString();
                if (resp != "OK")
                {
                    Console.WriteLine($"DOWNLOAD -> {resp}");
                    return;
                }

                int length = reader.ReadInt32();
                byte[] data = reader.ReadBytes(length);
                File.WriteAllBytes(outputPath, data);

                Console.WriteLine($"DOWNLOAD OK -> {outputPath}");
            });
        }

        public void Shutdown()
        {
            Send((writer, reader) =>
            {
                writer.Write("SHUTDOWN");
                Console.WriteLine($"Shutdown -> {reader.ReadString()}");
            });
        }

        private void Send(Action<BinaryWriter, BinaryReader> action)
        {
            try
            {
                using (TcpClient client = new TcpClient(Config.HOST, port))
                using (NetworkStream stream = client.GetStream())
                using (BinaryReader reader = new BinaryReader(stream, Encoding.UTF8, true))
                using (BinaryWriter writer = new BinaryWriter(stream, Encoding.UTF8, true))
                {
                    action(writer, reader);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro de comunicação: {ex.Message}");
            }
        }

        // recebe args a partir do índice após o token "client"
        public static void RunAsClient(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine(
                    "Uso:\n" +
                    " dotnet run -- client <porta> upload <arquivo>\n" +
                    " dotnet run -- client <porta> download <nome> <saida>\n" +
                    " dotnet run -- client <porta> list\n" +
                    " dotnet run -- client <porta> shutdown\n"
                );
                return;
            }

            int port = int.Parse(args[0]);
            Client client = new Client(port);

            switch (args[1])
            {
                case "upload": client.Upload(args[2]); break;
                case "download": client.Download(args[2], args[3]); break;
                case "list": client.List(); break;
                case "shutdown": client.Shutdown(); break;
                default: Console.WriteLine("Comando inválido"); break;
            }
        }
    }
}