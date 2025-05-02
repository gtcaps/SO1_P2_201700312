FROM golang:1.18

WORKDIR /app

# Copiar archivos del proyecto
COPY . .

# Crear módulo Go si no existe
RUN go mod tidy

# Compilar
RUN go build -o rabbit-consumer

# Iniciar aplicación
CMD ["./rabbit-consumer"]