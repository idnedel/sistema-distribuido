using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace DistributedStorageSystem
{
    public class Client
    {
        //armazenar porta do nó alvo
        private readonly int port;

        public Client(int port)
        {
            this.port = port;
        }

        //upload de arquivo
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

        //listar arquivos
        public void List()
        {
            Send((writer, reader) =>
            {
                writer.Write("LISTALL");

                string resp = reader.ReadString();
                if (resp != "OK")
                {
                    Console.WriteLine($"LIST -> {resp}");
                    return;
                }

                //qtd de arquivos
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

        //baixar arquivo
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
        //derrubar nó
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
                //conexão tcp
                using (TcpClient client = new TcpClient(Config.HOST, port))
                //obtem fluxo de rede
                using (NetworkStream stream = client.GetStream())
                //leitores e escritores binários para o fluxo
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

        // recebe args
        public static void RunAsClient(string[] args)
        {
            //valida numero minimo de args
            if (args.Length < 2)
            {
                Console.WriteLine("Digite o comando...");
                return;
            }

            //parse da porta do nó
            int port = int.Parse(args[0]);

            //cria cliente
            Client client = new Client(port);

            //segundo args determina ação
            switch (args[1])
            {
                case 
                    "upload": 
                        client.Upload(args[2]); break;
                case 
                    "download": 
                        client.Download(args[2], args[3]); break;
                case
                    "list": 
                        client.List(); break;
                case
                    "shutdown": 
                        client.Shutdown(); break;
                default:
                        Console.WriteLine("Comando inválido"); break;
            }
        }
    }
}