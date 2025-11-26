iniciar os nós
dotnet run

upload
dotnet run -- client 6060 upload "D:\PROGS\sistema-distribuido\sistema-distribuido\texto.txt"
dotnet run -- client 6060 upload "D:\PROGS\sistema-distribuido\sistema-distribuido\paisagem.png"

listar
dotnet run -- client 6061 list

download
dotnet run -- client 6062 download "texto.txt" "D:\PROGS\sistema-distribuido\sistema-distribuido\texto-download.txt"
dotnet run -- client 6062 download "paisagem.png" "D:\PROGS\sistema-distribuido\sistema-distribuido\paisagem-download.png"

derrubar nó
dotnet run -- client 6063 shutdown
