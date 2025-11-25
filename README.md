Compilar
dotnet build

Iniciar todos os nós via launcher
dotnet run

Em outro terminal: fazer upload
dotnet run -- client 6060 upload "D:\PROGS\sistema-distribuido\sistema-distribuido\texto.txt"
dotnet run -- client 6060 upload "D:\PROGS\sistema-distribuido\sistema-distribuido\paisagem.png"

Listar
dotnet run -- client 6061 list

Download
dotnet run -- client 6062 download "texto.txt" "D:\PROGS\sistema-distribuido\sistema-distribuido\texto-download.txt"
dotnet run -- client 6062 download "paisagem.png" "D:\PROGS\sistema-distribuido\sistema-distribuido\paisagem-download.png"

Derrubar nó 6063
dotnet run -- client 6063 shutdown
