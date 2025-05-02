package main

import (
    "log"
    "os"

    "go-api/api"
    "go-api/grpc_client"
)

func main() {
    // Obtener direcciones de los servicios gRPC de las variables de entorno
    rabbitAddr := os.Getenv("RABBIT_GRPC_ADDR")
    if rabbitAddr == "" {
        rabbitAddr = "rabbit-writer-service:50051"
    }

    kafkaAddr := os.Getenv("KAFKA_GRPC_ADDR")
    if kafkaAddr == "" {
        kafkaAddr = "kafka-writer-service:50052"
    }

    // Crear cliente gRPC
    client, err := grpc_client.NewGrpcClient(rabbitAddr, kafkaAddr)
    if err != nil {
        log.Fatalf("Error al crear cliente gRPC: %v", err)
    }

    // Crear y configurar el servidor API
    apiServer := api.NewAPIServer(client)
    router := apiServer.SetupRouter()

    // Obtener puerto del entorno o usar 8080 por defecto
    port := os.Getenv("PORT")
    if port == "" {
        port = "8080"
    }

    // Iniciar el servidor HTTP
    log.Printf("API REST iniciada en :%s", port)
    if err := router.Run(":" + port); err != nil {
        log.Fatalf("Error al iniciar el servidor: %v", err)
    }
}