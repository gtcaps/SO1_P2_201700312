package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net"
    "os"
    "time"

    "github.com/streadway/amqp"
    "google.golang.org/grpc"
    "rabbit-writer/proto"
)

type weatherData struct {
    Description string `json:"description"`
    Country     string `json:"country"`
    Weather     string `json:"weather"`
}

// RabbitMQWriter implementa la interfaz del servicio gRPC
type RabbitMQWriter struct {
    proto.UnimplementedWeatherServiceServer
    conn    *amqp.Connection
    channel *amqp.Channel
    queue   string
}

// NewRabbitMQWriter crea una nueva instancia del servicio RabbitMQWriter
func NewRabbitMQWriter(amqpURI, queueName string) (*RabbitMQWriter, error) {
    // Intentar conectar con reintentos
    var conn *amqp.Connection
    var err error
    retries := 5

    for i := 0; i < retries; i++ {
        conn, err = amqp.Dial(amqpURI)
        if err == nil {
            break
        }
        log.Printf("Error al conectar a RabbitMQ (intento %d/%d): %v", i+1, retries, err)
        time.Sleep(5 * time.Second)
    }

    if conn == nil {
        return nil, fmt.Errorf("no se pudo conectar a RabbitMQ después de %d intentos", retries)
    }

    ch, err := conn.Channel()
    if err != nil {
        return nil, err
    }

    q, err := ch.QueueDeclare(
        queueName, // nombre
        true,      // durable
        false,     // delete when unused
        false,     // exclusive
        false,     // no-wait
        nil,       // arguments
    )
    if err != nil {
        return nil, err
    }

    return &RabbitMQWriter{
        conn:    conn,
        channel: ch,
        queue:   q.Name,
    }, nil
}

// Close cierra las conexiones con RabbitMQ
func (w *RabbitMQWriter) Close() {
    if w.channel != nil {
        w.channel.Close()
    }
    if w.conn != nil {
        w.conn.Close()
    }
}

// PublishToRabbitMQ implementa el método del servicio gRPC para publicar mensajes en RabbitMQ
func (w *RabbitMQWriter) PublishToRabbitMQ(ctx context.Context, req *proto.WeatherData) (*proto.PublishResponse, error) {
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

    err = w.channel.Publish(
        "",       // exchange
        w.queue,  // routing key
        false,    // mandatory
        false,    // immediate
        amqp.Publishing{
            ContentType: "application/json",
            Body:        jsonData,
        })
    if err != nil {
        return &proto.PublishResponse{
            Success: false,
            Message: fmt.Sprintf("Error al publicar en RabbitMQ: %v", err),
        }, nil
    }

    return &proto.PublishResponse{
        Success: true,
        Message: "Mensaje publicado en RabbitMQ correctamente",
    }, nil
}

func main() {
    // Obtener configuración de entorno
    rabbitURI := os.Getenv("RABBITMQ_URI")
    if rabbitURI == "" {
        rabbitURI = "amqp://guest:guest@rabbitmq:5672/"
    }

    queueName := os.Getenv("RABBITMQ_QUEUE")
    if queueName == "" {
        queueName = "message"
    }

    port := os.Getenv("PORT")
    if port == "" {
        port = "50051"
    }

    // Crear el servicio RabbitMQ Writer
    writer, err := NewRabbitMQWriter(rabbitURI, queueName)
    if err != nil {
        log.Fatalf("Error al crear RabbitMQ Writer: %v", err)
    }
    defer writer.Close()

    // Crear servidor gRPC
    grpcServer := grpc.NewServer()
    proto.RegisterWeatherServiceServer(grpcServer, writer)

    // Escuchar en el puerto especificado
    lis, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("Error al escuchar: %v", err)
    }

    log.Printf("Servidor gRPC RabbitMQ Writer iniciado en :%s", port)
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Error al servir: %v", err)
    }
}