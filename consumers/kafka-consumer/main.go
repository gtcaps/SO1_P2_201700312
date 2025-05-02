package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "strings"
    "sync"
    "syscall"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/segmentio/kafka-go"
)

// WeatherData representa los datos del clima recibidos
type WeatherData struct {
    Description string `json:"description"`
    Country     string `json:"country"`
    Weather     string `json:"weather"`
}

// KafkaConsumer consume mensajes de Kafka
type KafkaConsumer struct {
    reader *kafka.Reader
    redis  *redis.Client
    ctx    context.Context
}

// NewKafkaConsumer crea un nuevo consumidor de Kafka
func NewKafkaConsumer(brokers []string, topic, groupID string, redisAddr string) (*KafkaConsumer, error) {
    // Configurar el reader de Kafka
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:        brokers,
        Topic:          topic,
        GroupID:        groupID,
        MinBytes:       10e3,        // 10KB
        MaxBytes:       10e6,        // 10MB
        CommitInterval: time.Second, // Confirmar offsets cada segundo
    })

    // Conectar a Redis
    redisClient := redis.NewClient(&redis.Options{
        Addr:     redisAddr,
        Password: "", // sin contraseña
        DB:       0,  // usar base de datos 0
    })

    // Verificar conexión a Redis
    ctx := context.Background()
    _, err := redisClient.Ping(ctx).Result()
    if err != nil {
        return nil, err
    }

    return &KafkaConsumer{
        reader: reader,
        redis:  redisClient,
        ctx:    ctx,
    }, nil
}

// Close cierra las conexiones
func (c *KafkaConsumer) Close() {
    if c.reader != nil {
        c.reader.Close()
    }
    if c.redis != nil {
        c.redis.Close()
    }
}

// Start inicia el consumo de mensajes
func (c *KafkaConsumer) Start(workerCount int) error {
    // Canal para señales de terminación
    stopChan := make(chan os.Signal, 1)
    signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

    // Canal para mensajes de Kafka
    messageChan := make(chan kafka.Message, 100)

    // Goroutine para leer mensajes de Kafka
    go func() {
        defer close(messageChan)
        for {
            message, err := c.reader.ReadMessage(c.ctx)
            if err != nil {
                log.Printf("Error al leer mensaje de Kafka: %v", err)
                select {
                case <-stopChan:
                    return
                default:
                    time.Sleep(1 * time.Second)
                    continue
                }
            }
            select {
            case messageChan <- message:
                // Mensaje enviado al canal
            case <-stopChan:
                return
            }
        }
    }()

    // WaitGroup para esperar a que finalicen todos los workers
    var wg sync.WaitGroup
    wg.Add(workerCount)

    // Iniciar workers goroutines
    for i := 0; i < workerCount; i++ {
        go func(workerID int) {
            defer wg.Done()
            log.Printf("Worker %d iniciado", workerID)

            for {
                select {
                case message, ok := <-messageChan:
                    if !ok {
                        log.Printf("Worker %d: canal de mensajes cerrado", workerID)
                        return
                    }

                    // Procesar el mensaje
                    err := c.processMessage(message.Value)
                    if err != nil {
                        log.Printf("Worker %d: error al procesar mensaje: %v", workerID, err)
                    }

                case <-stopChan:
                    log.Printf("Worker %d: señal de terminación recibida", workerID)
                    return
                }
            }
        }(i)
    }

    // Esperar la señal de terminación
    <-stopChan
    log.Println("Iniciando apagado graceful...")

    // Esperar a que los workers terminen
    wg.Wait()
    log.Println("Todos los workers han finalizado")
    return nil
}

// processMessage procesa un mensaje recibido de Kafka
func (c *KafkaConsumer) processMessage(value []byte) error {
    var weatherData WeatherData
    if err := json.Unmarshal(value, &weatherData); err != nil {
        return err
    }

    // Incrementar contador por país
    countryKey := "country:" + strings.ToLower(weatherData.Country)
    if err := c.redis.HIncrBy(c.ctx, "weather_stats", countryKey, 1).Err(); err != nil {
        return err
    }

    // Incrementar contador por tipo de clima
    weatherKey := "weather:" + strings.ToLower(weatherData.Weather)
    if err := c.redis.HIncrBy(c.ctx, "weather_stats", weatherKey, 1).Err(); err != nil {
        return err
    }

    // Incrementar contador total
    if err := c.redis.HIncrBy(c.ctx, "weather_stats", "total", 1).Err(); err != nil {
        return err
    }

    log.Printf("Procesado: País=%s, Clima=%s", weatherData.Country, weatherData.Weather)
    return nil
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

    kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
    if kafkaGroupID == "" {
        kafkaGroupID = "weather-consumer-group"
    }

    redisAddr := os.Getenv("REDIS_ADDR")
    if redisAddr == "" {
        redisAddr = "redis:6379"
    }

    // Obtener número de workers (goroutines)
    workerCount := 5 // valor por defecto

    // Crear el consumidor
    consumer, err := NewKafkaConsumer([]string{kafkaBroker}, kafkaTopic, kafkaGroupID, redisAddr)
    if err != nil {
        log.Fatalf("Error al crear consumidor de Kafka: %v", err)
    }
    defer consumer.Close()

    // Iniciar el consumo de mensajes
    log.Printf("Iniciando consumidor de Kafka con %d workers", workerCount)
    if err := consumer.Start(workerCount); err != nil {
        log.Fatalf("Error al iniciar el consumidor: %v", err)
    }
}