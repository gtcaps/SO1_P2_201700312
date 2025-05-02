FROM golang:1.18

WORKDIR /app

# Copiar todos los archivos del proyecto
COPY . .

# Instalar protobuf
RUN apt-get update && apt-get install -y protobuf-compiler

# Generar archivos protobuf manualmente
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
RUN export PATH="$PATH:$(go env GOPATH)/bin"

# Intentar compilar con las dependencias necesarias
RUN go mod tidy
RUN go build

# Exponer puerto
EXPOSE 8080

# Iniciar aplicación (usa el nombre del módulo como nombre del binario)
CMD ["./go-api"]