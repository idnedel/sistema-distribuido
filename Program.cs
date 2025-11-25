using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace DistributedStorageSystem
{
    class Program
    {
        static void Main(string[] args)
        {
            // se houver args, executa como node/client; caso contrário continua como launcher.
            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "client":
                        Client.RunAsClient(args.Skip(1).ToArray());
                        return;
                    case "node":
                        StorageNode.RunAsNode(args.Skip(1).ToArray());
                        return;
                    default:
                        break;
                }
            }

            // limpar storage
            var storageRoot = new DirectoryInfo("storage");
            if (storageRoot.Exists)
            {
                foreach (var nodeDir in storageRoot.GetDirectories())
                {
                    foreach (var file in nodeDir.GetFiles()) file.Delete();
                    nodeDir.Delete();
                }
            }

            Console.WriteLine("Storages limpos!\n");
            Console.WriteLine("Iniciando nós em processos separados...\n");

            // compilar o projeto primeiro
            var buildProcess = Process.Start("dotnet", "build");
            buildProcess.WaitForExit();

            string projectPath;
            try
            {
                projectPath = GetProjectPath();
            }
            catch (FileNotFoundException ex)
            {
                Console.WriteLine($"Erro: {ex.Message}");
                return;
            }

            foreach (int port in Config.PORTS)
            {
                try
                {
                    ProcessStartInfo startInfo = new ProcessStartInfo
                    {
                        FileName = "dotnet",
                        Arguments = $"run --project \"{projectPath}\" -- node {port}",
                        UseShellExecute = true,
                        CreateNoWindow = false,
                        WorkingDirectory = Directory.GetCurrentDirectory()
                    };

                    Process.Start(startInfo);
                    Console.WriteLine($"→ Nó {port} iniciado.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Falha ao iniciar nó {port}: {ex.Message}");
                }
            }

            Console.WriteLine("\nTodos os nós foram iniciados como processos independentes.");
            Console.WriteLine("Pressione qualquer tecla para encerrar este launcher...");
            Console.ReadKey();
        }

        private static string GetProjectPath()
        {
            string cwd = Directory.GetCurrentDirectory();
            // nome real do .csproj no repositório
            string expected = Path.Combine(cwd, "sistema-distribuido.csproj");
            if (File.Exists(expected)) return expected;

            //procura qualquer .csproj no diretório atual
            var csproj = Directory.EnumerateFiles(cwd, "*.csproj").FirstOrDefault();
            if (csproj != null) return csproj;

            throw new FileNotFoundException("Arquivo .csproj não encontrado no diretório atual.", cwd);
        }
    }
}