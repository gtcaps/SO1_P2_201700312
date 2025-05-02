package api

import (
    "context"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "go-api/grpc_client"
    "go-api/proto"
)

// Struct
type WeatherData struct {
    Description string `json:"description"`
    Country     string `json:"country"`
    Weather     string `json:"weather"`
}

// Server
type APIServer struct {
    GrpcClient *grpc_client.GrpcClient
}

// NewAPIServer crea una nueva instancia del servidor API
func NewAPIServer(grpcClient *grpc_client.GrpcClient) *APIServer {
    return &APIServer{
        GrpcClient: grpcClient,
    }
}

// SetupRouter configura las rutas del router Gin
func (s *APIServer) SetupRouter() *gin.Engine {
    r := gin.Default()

    // Middleware para CORS
    r.Use(func(c *gin.Context) {
        c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
        c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
        c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, Authorization")
        if c.Request.Method == "OPTIONS" {
            c.AbortWithStatus(204)
            return
        }
        c.Next()
    })

    // Endpoint para recibir datos del clima
    r.POST("/weather", s.handleWeatherData)

    // Endpoint de salud
    r.GET("/health", func(c *gin.Context) {
        c.String(http.StatusOK, "Healthy!")
    })

    return r
}

// handleWeatherData maneja las peticiones POST con datos del clima
func (s *APIServer) handleWeatherData(c *gin.Context) {
    var weatherData WeatherData

    if err := c.ShouldBindJSON(&weatherData); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{
            "error": "Invalid request format",
        })
        return
    }

    // Crear contexto con timeout
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Convertir a formato protobuf
    protoData := &proto.WeatherData{
        Description: weatherData.Description,
        Country:     weatherData.Country,
        Weather:     weatherData.Weather,
    }

    // RabbitMQ
    rabbitResp, err := s.GrpcClient.PublishToRabbitMQ(ctx, protoData)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{
            "error": "Failed to publish to RabbitMQ: " + err.Error(),
        })
        return
    }

    // Kafka
    kafkaResp, err := s.GrpcClient.PublishToKafka(ctx, protoData)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{
            "error": "Failed to publish to Kafka: " + err.Error(),
        })
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "rabbitmq": rabbitResp,
        "kafka":    kafkaResp,
    })
}