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

        public StorageNode(int port)
        {
            this.port = port;
            this.storageDir = Path.Combine("storage", $"node_{port}");
            Directory.CreateDirectory(storageDir);
            LoadMetadataFromDisk();
        }

        public static void RunAsNode(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Uso: dotnet run -- node <porta>");
                return;
            }

            int port = int.Parse(args[0]);
            new StorageNode(port).Start();
        }

        public void Start()
        {
            try
            {
                TcpListener server = new TcpListener(IPAddress.Parse(Config.HOST), port);
                server.Start();

                Console.WriteLine($"[NODE {port}] Online. Nodes={Config.PortsString}");

                while (true)
                {
                    TcpClient client = server.AcceptTcpClient();
                    ThreadPool.QueueUserWorkItem(HandleClient, client);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NODE {port}] Erro start: {ex.Message}");
            }
        }

        private void HandleClient(object? state)
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
                        case "UPLOAD": HandleUpload(reader, writer); break;
                        case "PUT_PART": HandlePutPart(reader, writer); break;
                        case "GET_PART": HandleGetPart(reader, writer); break;
                        case "PUT_META": HandlePutMeta(reader, writer); break;
                        case "LIST_GLOBAL": HandleList(writer); break;
                        case "DOWNLOAD": HandleDownload(reader, writer); break;
                        case "SHUTDOWN":
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

        private void HandleUpload(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int size = reader.ReadInt32();
            byte[] data = reader.ReadBytes(size);

            int parts = (int)Math.Ceiling(size / (double)Config.CHUNK_SIZE);

            PutMetaLocal(filename, parts, size);
            ReplicateMeta(filename, parts, size);

            // fragmenta e distribui
            for (int i = 0; i < parts; i++)
            {
                int start = i * Config.CHUNK_SIZE;
                int end = Math.Min(start + Config.CHUNK_SIZE, size);
                byte[] chunk = new byte[end - start];
                Array.Copy(data, start, chunk, 0, chunk.Length);
                DistributePart(filename, i, chunk);
            }

            writer.Write("OK");
        }

        private void DistributePart(string filename, int partIndex, byte[] chunk)
        {
            int n = Config.PORTS.Length;
            int primaryNode = partIndex % n;
            int backupNode = (partIndex + 1) % n;

            SendToNode(Config.PORTS[primaryNode], filename, partIndex, chunk);
            SendToNode(Config.PORTS[backupNode], filename, partIndex, chunk);
        }

        private void SendToNode(int targetPort, string filename, int partIndex, byte[] chunk)
        {
            try
            {
                Send(targetPort, (writer, reader) =>
                {
                    writer.Write("PUT_PART");
                    writer.Write(filename);
                    writer.Write(partIndex);
                    writer.Write(chunk.Length);
                    writer.Write(chunk);
                    reader.ReadString(); // OK
                });
                Console.WriteLine($"[NODE {port}] Part {partIndex} -> {targetPort}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NODE {port}] Failed to send to {targetPort}: {ex.Message}");
            }
        }

        private void HandlePutPart(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int partIndex = reader.ReadInt32();
            int len = reader.ReadInt32();
            byte[] chunk = reader.ReadBytes(len);

            string filePath = Path.Combine(storageDir, $"{filename}.part{partIndex}");
            File.WriteAllBytes(filePath, chunk);

            writer.Write("OK");
        }

        private void HandleGetPart(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int partIndex = reader.ReadInt32();

            string filePath = Path.Combine(storageDir, $"{filename}.part{partIndex}");
            if (!File.Exists(filePath))
            {
                writer.Write("ERR");
                return;
            }

            byte[] data = File.ReadAllBytes(filePath);
            writer.Write("OK");
            writer.Write(data.Length);
            writer.Write(data);
        }

        private void HandlePutMeta(BinaryReader reader, BinaryWriter writer)
        {
            string filename = reader.ReadString();
            int parts = reader.ReadInt32();
            long size = reader.ReadInt64();

            PutMetaLocal(filename, parts, size);
            writer.Write("OK");
        }

        private void PutMetaLocal(string filename, int parts, long size)
        {
            lock (metadataLock)
            {
                metadata[filename] = new FileMeta(parts, size);
                SaveMetadataToDisk();
            }
        }

        private void ReplicateMeta(string filename, int parts, long size)
        {
            foreach (int p in Config.PORTS)
            {
                if (p == port) continue;

                try
                {
                    Send(p, (writer, reader) =>
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

        private void HandleList(BinaryWriter writer)
        {
            lock (metadataLock)
            {
                var files = metadata.Keys.OrderBy(k => k).ToList();
                writer.Write("OK");
                writer.Write(files.Count);
                foreach (string file in files) writer.Write(file);
            }
        }

        private void HandleDownload(BinaryReader reader, BinaryWriter writer)
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
                byte[]? part = FetchPart(filename, i);
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

        private byte[]? FetchPart(string filename, int partIndex)
        {
            int n = Config.PORTS.Length;
            int primaryNode = partIndex % n;
            int backupNode = (partIndex + 1) % n;

            foreach (int targetPort in new[] { Config.PORTS[primaryNode], Config.PORTS[backupNode] })
            {
                try
                {
                    byte[]? partData = null;
                    Send(targetPort, (writer, reader) =>
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

        private void Send(int port, Action<BinaryWriter, BinaryReader> action)
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

        private string MetaFilePath => Path.Combine(storageDir, "metadata.db");

        private void SaveMetadataToDisk()
        {
            lock (metadataLock)
            {
                using (StreamWriter sw = new StreamWriter(MetaFilePath))
                {
                    foreach (var entry in metadata)
                    {
                        sw.WriteLine($"{entry.Key}|{entry.Value.Parts}|{entry.Value.TotalSize}");
                    }
                }
            }
        }

        private void LoadMetadataFromDisk()
        {
            string filePath = MetaFilePath;
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