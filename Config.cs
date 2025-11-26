namespace DistributedStorageSystem
{
    public static class Config
    {
        // 4 nós
        public static readonly int[] PORTS = { 6060, 6061, 6062, 6063 };
        public static readonly string HOST = "0.0.0.0";

        // tamanho do fragmento 128kb
        public static readonly int FRAG_SIZE = 128 * 1024;

        // 2 réplicas = tolera 1 nó caído / fechado
        public static readonly int REPLICA = 2;

        public static int NodeCount => PORTS.Length;

        public static string PortsString => $"[{string.Join(", ", PORTS)}]";
    }
}