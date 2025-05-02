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
    "github.com/streadway/amqp"
)

// WeatherData representa los datos del clima recibidos
type WeatherData struct {
    Description string `json:"description"`
    Country     string `json:"country"`
    Weather     string `json:"weather"`
}

// RabbitMQConsumer consume mensajes de RabbitMQ
type RabbitMQConsumer struct {
    conn    *amqp.Connection
    channel *amqp.Channel
    queue   string
    redis   *redis.Client
    ctx     context.Context
}

// NewRabbitMQConsumer crea un nuevo consumidor de RabbitMQ
func NewRabbitMQConsumer(amqpURI, queueName string, redisAddr string) (*RabbitMQConsumer, error) {
    // Conectar a RabbitMQ con reintentos
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
        return nil, err
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

    // Conectar a Redis/Valkey
    redisClient := redis.NewClient(&redis.Options{
        Addr:     redisAddr,
        Password: "", // sin contraseña
        DB:       0,  // usar base de datos 0
    })

    // Verificar conexión a Redis
    ctx := context.Background()
    _, err = redisClient.Ping(ctx).Result()
    if err != nil {
        return nil, err
    }

    return &RabbitMQConsumer{
        conn:    conn,
        channel: ch,
        queue:   q.Name,
        redis:   redisClient,
        ctx:     ctx,
    }, nil
}

// Close cierra las conexiones
func (c *RabbitMQConsumer) Close() {
    if c.channel != nil {
        c.channel.Close()
    }
    if c.conn != nil {
        c.conn.Close()
    }
    if c.redis != nil {
        c.redis.Close()
    }
}

// Start inicia el consumo de mensajes
func (c *RabbitMQConsumer) Start(workerCount int) error {
    // Configurar el canal para recibir mensajes
    msgs, err := c.channel.Consume(
        c.queue, // cola
        "",      // consumer
        false,   // auto-ack
        false,   // exclusive
        false,   // no-local
        false,   // no-wait
        nil,     // args
    )
    if err != nil {
        return err
    }

    // Canal para señales de terminación
    stopChan := make(chan os.Signal, 1)
    signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

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
                case d, ok := <-msgs:
                    if !ok {
                        log.Printf("Worker %d: canal de mensajes cerrado", workerID)
                        return
                    }

                    // Procesar el mensaje
                    err := c.processMessage(d.Body)
                    if err != nil {
                        log.Printf("Worker %d: error al procesar mensaje: %v", workerID, err)
                        d.Nack(false, true) // rechazar y reintentar
                    } else {
                        d.Ack(false) // confirmar procesamiento
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

    // Cerrar el canal y esperar a que los workers terminen
    if err := c.channel.Cancel("", false); err != nil {
        log.Printf("Error al cancelar el consumidor: %v", err)
    }

    wg.Wait()
    log.Println("Todos los workers han finalizado")
    return nil
}

// processMessage procesa un mensaje recibido de RabbitMQ
func (c *RabbitMQConsumer) processMessage(body []byte) error {
    var weatherData WeatherData
    if err := json.Unmarshal(body, &weatherData); err != nil {
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
    rabbitURI := os.Getenv("RABBITMQ_URI")
    if rabbitURI == "" {
        rabbitURI = "amqp://guest:guest@rabbitmq:5672/"
    }

    queueName := os.Getenv("RABBITMQ_QUEUE")
    if queueName == "" {
        queueName = "message"
    }

    valkeyAddr := os.Getenv("VALKEY_ADDR")
    if valkeyAddr == "" {
        valkeyAddr = "valkey:6379"
    }

    // Obtener número de workers (goroutines)
    workerCount := 5 // valor por defecto

    // Crear el consumidor
    consumer, err := NewRabbitMQConsumer(rabbitURI, queueName, valkeyAddr)
    if err != nil {
        log.Fatalf("Error al crear consumidor de RabbitMQ: %v", err)
    }
    defer consumer.Close()

    // Iniciar el consumo de mensajes
    log.Printf("Iniciando consumidor de RabbitMQ con %d workers", workerCount)
    if err := consumer.Start(workerCount); err != nil {
        log.Fatalf("Error al iniciar el consumidor: %v", err)
    }
}