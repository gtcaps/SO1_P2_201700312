package grpc_client

import (
    "context"
    "log"

    "go-api/proto"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)


type GrpcClient struct {
    rabbitClient proto.WeatherServiceClient
    kafkaClient  proto.WeatherServiceClient
}

func NewGrpcClient(rabbitAddr, kafkaAddr string) (*GrpcClient, error) {
    // Conexión al servicio RabbitMQ Writer
    rabbitConn, err := grpc.Dial(rabbitAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    rabbitClient := proto.NewWeatherServiceClient(rabbitConn)

    // Conexión al servicio Kafka Writer
    kafkaConn, err := grpc.Dial(kafkaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    kafkaClient := proto.NewWeatherServiceClient(kafkaConn)

    return &GrpcClient{
        rabbitClient: rabbitClient,
        kafkaClient:  kafkaClient,
    }, nil
}

// PublishToRabbitMQ envía datos a RabbitMQ a través de gRPC
func (c *GrpcClient) PublishToRabbitMQ(ctx context.Context, data *proto.WeatherData) (*proto.PublishResponse, error) {
    resp, err := c.rabbitClient.PublishToRabbitMQ(ctx, data)
    if err != nil {
        log.Printf("Error al publicar en RabbitMQ: %v", err)
        return nil, err
    }
    return resp, nil
}

// PublishToKafka envía datos a Kafka a través de gRPC
func (c *GrpcClient) PublishToKafka(ctx context.Context, data *proto.WeatherData) (*proto.PublishResponse, error) {
    resp, err := c.kafkaClient.PublishToKafka(ctx, data)
    if err != nil {
        log.Printf("Error al publicar en Kafka: %v", err)
        return nil, err
    }
    return resp, nil
}