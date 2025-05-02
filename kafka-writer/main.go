package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net"
    "os"
    "time"

    "github.com/segmentio/kafka-go"
    "google.golang.org/grpc"
    "kafka-writer/proto"
)

type weatherData struct {
    Description string `json:"description"`
    Country     string `json:"country"`
    Weather     string `json:"weather"`
}

// KafkaWriter implementa la interfaz del servicio gRPC
type KafkaWriter struct {
    proto.UnimplementedWeatherServiceServer
    writer *kafka.Writer
}

// NewKafkaWriter crea una nueva instancia del servicio KafkaWriter
func NewKafkaWriter(brokers []string, topic string) *KafkaWriter {
    // Configurar el writer de Kafka
    writer := &kafka.Writer{
        Addr:         kafka.TCP(brokers...),
        Topic:        topic,
        Balancer:     &kafka.LeastBytes{},
        RequiredAcks: kafka.RequireOne,
        // Configuración adicional para mejorar la performance
        BatchSize:    100,
        BatchTimeout: 10 * time.Millisecond,
    }

    return &KafkaWriter{
        writer: writer,
    }
}

// Close cierra la conexión con Kafka
func (w *KafkaWriter) Close() {
    if w.writer != nil {
        w.writer.Close()
    }
}

// PublishToKafka implementa el método del servicio gRPC para publicar mensajes en Kafka
func (w *KafkaWriter) PublishToKafka(ctx context.Context, req *proto.WeatherData) (*proto.PublishResponse, error) {
    data := weatherData{
        Description: req.Description,
        Country:     req.Country,
        Weather:     req.Weather,
    }

    jsonData, err := json.Marshal(data)
    if err != nil {
        return &proto.PublishResponse{
            Success: false,
            Message: fmt.Sprintf("Error al serializar datos: %v", err),
        }, nil
    }

    // Publicar mensaje en Kafka
    err = w.writer.WriteMessages(ctx,
        kafka.Message{
            Key:   []byte(data.Country),  // Usar el país como clave para particionamiento
            Value: jsonData,
        },
    )
    if err != nil {
        return &proto.PublishResponse{
            Success: false,
            Message: fmt.Sprintf("Error al publicar en Kafka: %v", err),
        }, nil
    }

    return &proto.PublishResponse{
        Success: true,
        Message: "Mensaje publicado en Kafka correctamente",
    }, nil
}

func main() {
    // Obtener configuración de entorno
    kafkaBroker := os.Getenv("KAFKA_BROKER")
    if kafkaBroker == "" {
        kafkaBroker = "kafka:9092"
    }

    kafkaTopic := os.Getenv("KAFKA_TOPIC")
    if kafkaTopic == "" {
        kafkaTopic = "message"
    }

    port := os.Getenv("PORT")
    if port == "" {
        port = "50052"
    }

    // Crear el servicio Kafka Writer
    writer := NewKafkaWriter([]string{kafkaBroker}, kafkaTopic)
    defer writer.Close()

    // Crear servidor gRPC
    grpcServer := grpc.NewServer()
    proto.RegisterWeatherServiceServer(grpcServer, writer)

    // Escuchar en el puerto especificado
    lis, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("Error al escuchar: %v", err)
    }

    log.Printf("Servidor gRPC Kafka Writer iniciado en :%s", port)
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Error al servir: %v", err)
    }
}