using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedStorageSystem
{
    public class StorageNode
    {
        private readonly int port;
        private readonly string storageDir;
        private readonly Dictionary<string, FileMeta> metadata = new Dictionary<string, FileMeta>();
        private readonly object metadataLock = new object();

        // metadados de arquivo, numero de fragmentos e tamanho em bytes
        public class FileMeta
        {
            public int Parts { get; }
            public long TotalSize { get; }

            public FileMeta(int parts, long totalSize)
            {
                Parts = parts;
                TotalSize = totalSize;
            }
        }

        // inicia nó, carrega metadados do disco para memória
        public StorageNode(int port)
        {
            this.port = port;
            this.storageDir = Path.Combine("storage", $"node_{port}");
            Directory.CreateDirectory(storageDir);
            CarregaMetadadosDoDisco();
        }

        public static void ExecutarComoNo(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Uso: dotnet run -- node <porta>");
                return;
            }

            int port = int.Parse(args[0]);
            new StorageNode(port).Iniciar();
        }

        // inicia servidor tcp na porta especificada
        public void Iniciar()
        {
            try
            {
                TcpListener server = new TcpListener(IPAddress.Parse(Config.HOST), port);
                server.Start();

                Console.WriteLine($"[NODE {port}] Online. Nodes={Config.PortsString}");

                // aceita conexões de clientes
                while (true)
                {
                    TcpClient client = server.AcceptTcpClient();
                    // cada cliente em paralelo
                    ThreadPool.QueueUserWorkItem(TrataCliente, client);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NODE {port}] Erro start: {ex.Message}");
            }
        }

        // manipula cliente conectado
        private void TrataCliente(object? state)
        {
            TcpClient? client = state as TcpClient;
            if (client == null) return;

            try
            {
                using (client)
                using (NetworkStream stream = client.GetStream())
                using (BinaryReader reader = new BinaryReader(stream, Encoding.UTF8, true))
                using (BinaryWriter writer = new BinaryWriter(stream, Encoding.UTF8, true))
                {
                    string cmd = reader.ReadString();

                    switch (cmd)
                    {
                        case 
                            "UPLOAD": 
                                TrataUpload(reader, writer); break;
                        case 
                            "PUT_PART": 
                                TrataColocaParte(reader, writer); break;
                        case 
                            "GET_PART": 
                                TrataObtemParte(reader, writer); break;
                        case 
                            "PUT_META": 
                                TrataColocaMeta(reader, writer); break;
                        case 
                            "LISTALL": 
                                TrataLista(writer); break;
                        case 
                            "DOWNLOAD": 
                                TrataDownload(reader, writer); break;
                        case 
                            "SHUTDOWN":
                                Console.WriteLine($"[NODE {port}] Encerrando...");
                                writer.Write("OK");
                                Environment.Exit(0);
                            break;
                        default:
                            writer.Write("ERR Unknown command");
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NODE {port}] Erro handle: {ex.Message}");
            }
        }

        private void TrataUpload(BinaryReader reader, BinaryWriter writer)
        {
            // recebe dados do cliente
            string filename = reader.ReadString();
            int size = reader.ReadInt32();
            byte[] data = reader.ReadBytes(size);

            // calcula fragmentos
            int parts = (int)Math.Ceiling(size / (double)Config.CHUNK_SIZE);

            // armazena metadados local e replica
            ColocaMetaLocal(filename, parts, size);
            ReplicaMeta(filename, parts, size);

            // fragmenta em partes de 128KB e distribui
            for (int i = 0; i < parts; i++)
            {
                int start = i * Config.CHUNK_SIZE;
                int end = Math.Min(start + Config.CHUNK_SIZE, size);
                byte[] chunk = new byte[end - start];
                Array.Copy(data, start, chunk, 0, chunk.Length);
                DistribuiParte(filename, i, chunk); // distribui fragmento
            }

            writer.Write("OK");
        }

        // distribui fragmento para nós primário e backup
        private void DistribuiParte(string filename, int partIndex, byte[] chunk)
        {
            int n = Config.PORTS.Length;
            int primaryNode = partIndex % n; // primario
            int backupNode = (partIndex + 1) % n; // backup

            EnviaParaNo(Config.PORTS[primaryNode], filename, partIndex, chunk);
            EnviaParaNo(Config.PORTS[backupNode], filename, partIndex, chunk);
        }

        private void EnviaParaNo(int targetPort, string filename, int partIndex, byte[] chunk)
        {
            try
            {
                Envia(targetPort, (writer, reader) =>
                {
                    writer.Write("PUT_PART");
                    writer.Write(filename);
                    writer.Write(partIndex);
                    writer.Write(chunk.Length);
                    writer.Write(chunk);
                    reader.ReadString();
                });
                Console.WriteLine($"[NODE {port}] Part {partIndex} -> {targetPort}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NODE {port}] Failed to send to {targetPort}: {ex.Message}");
            }
        }

        // armazena fragmento localmente
        private void TrataColocaParte(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int partIndex = reader.ReadInt32();
            int len = reader.ReadInt32();
            byte[] chunk = reader.ReadBytes(len);

            string filePath = Path.Combine(storageDir, $"{filename}.part{partIndex}");
            File.WriteAllBytes(filePath, chunk);

            writer.Write("OK");
        }

        // recupera fragmento localmente
        private void TrataObtemParte(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int partIndex = reader.ReadInt32();

            string filePath = Path.Combine(storageDir, $"{filename}.part{partIndex}");
            if (!File.Exists(filePath))
            {
                writer.Write("ERR");
                return;
            }

            byte[] data = File.ReadAllBytes(filePath); // le do disco
            writer.Write("OK");
            writer.Write(data.Length);
            writer.Write(data);
        }

        // armazena metadados 
        private void TrataColocaMeta(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int parts = reader.ReadInt32();
            long size = reader.ReadInt64();

            ColocaMetaLocal(filename, parts, size); // armazena local
            writer.Write("OK");
        }

        // coloca metadados em memória e disco
        private void ColocaMetaLocal(string filename, int parts, long size)
        {
            lock (metadataLock)
            {
                metadata[filename] = new FileMeta(parts, size);
                SalvaMetadadosNoDisco();
            }
        }

        // replica metadados para outros nós e exclui ele mesmo
        private void ReplicaMeta(string filename, int parts, long size)
        {
            foreach (int p in Config.PORTS)
            {
                if (p == port) continue;

                try
                {
                    Envia(p, (writer, reader) =>
                    {
                        writer.Write("PUT_META");
                        writer.Write(filename);
                        writer.Write(parts);
                        writer.Write(size);
                        reader.ReadString();
                    });
                }
                catch (Exception) { }
            }
        }

        // listagem dos arquivos armazenados
        private void TrataLista(BinaryWriter writer)
        {
            lock (metadataLock)
            {
                var files = metadata.Keys.OrderBy(k => k).ToList();
                writer.Write("OK");
                writer.Write(files.Count);
                foreach (string file in files) writer.Write(file);
            }
        }

        // trata download, reagrupa fragmentos
        private void TrataDownload(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            FileMeta? meta;

            lock (metadataLock)
            {
                if (!metadata.TryGetValue(filename, out meta))
                {
                    writer.Write("ERR No such file");
                    return;
                }
            }

            byte[] full = new byte[meta!.TotalSize];

            int offset = 0;

            for (int i = 0; i < meta.Parts; i++)
            {
                byte[]? part = BuscaParte(filename, i);
                if (part == null)
                {
                    writer.Write($"ERR Missing part {i}");
                    return;
                }
                Array.Copy(part, 0, full, offset, part.Length);
                offset += part.Length;
            }

            writer.Write("OK");
            writer.Write(full.Length);
            writer.Write(full);
        }

        // busca fragmento em nós primário e backup se necessário
        private byte[]? BuscaParte(string filename, int partIndex)
        {
            int n = Config.PORTS.Length;
            int primaryNode = partIndex % n;
            int backupNode = (partIndex + 1) % n;

            foreach (int targetPort in new[] { Config.PORTS[primaryNode], Config.PORTS[backupNode] })
            {
                try
                {
                    byte[]? partData = null;
                    Envia(targetPort, (writer, reader) =>
                    {
                        writer.Write("GET_PART");
                        writer.Write(filename);
                        writer.Write(partIndex);

                        string resp = reader.ReadString();
                        if (resp == "OK")
                        {
                            int len = reader.ReadInt32();
                            partData = reader.ReadBytes(len);
                        }
                    });
                    if (partData != null) return partData;
                }
                catch (Exception) { }
            }
            return null;
        }

        // envia ação para nó na porta especificada
        private void Envia(int port, Action<BinaryWriter, BinaryReader> action)
        {
            using (TcpClient client = new TcpClient())
            {
                client.Connect(Config.HOST, port);
                client.ReceiveTimeout = 2000;
                client.SendTimeout = 2000;

                using (NetworkStream stream = client.GetStream())
                using (BinaryReader reader = new BinaryReader(stream, Encoding.UTF8, true))
                using (BinaryWriter writer = new BinaryWriter(stream, Encoding.UTF8, true))
                {
                    action(writer, reader);
                }
            }
        }

        private string CaminhoArquivoMeta => Path.Combine(storageDir, "metadata.db");

        
        private void SalvaMetadadosNoDisco()
        {
            lock (metadataLock)
            {
                using (StreamWriter sw = new StreamWriter(CaminhoArquivoMeta))
                {
                    foreach (var entry in metadata)
                    {
                        sw.WriteLine($"{entry.Key}|{entry.Value.Parts}|{entry.Value.TotalSize}");
                    }
                }
            }
        }

        private void CarregaMetadadosDoDisco()
        {
            string filePath = CaminhoArquivoMeta;
            if (!File.Exists(filePath)) return;

            lock (metadataLock)
            {
                foreach (string line in File.ReadAllLines(filePath))
                {
                    string[] parts = line.Split('|');
                    if (parts.Length == 3)
                    {
                        metadata[parts[0]] = new FileMeta(int.Parse(parts[1]), long.Parse(parts[2]));
                    }
                }
            }
        }
    }
}