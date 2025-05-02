Universidad de San Carlos de Guatemala <br>
Facultad de Ingenieria <br>
Escuela de Ciencias y Sistemas<br>
Laboratorio Sistemas de Bases de Datos 1 Seccion N<br>
Aybson Diddiere Mercado Grijalva<br>
201700312


# Proyecto 2 - Sistemas Operativos

Arquitectura distribuida en Google Cloud Platform que utiliza Kubernetes (GKE) para procesar y visualizar tweets relacionados con el clima mundial. 

## Componentes Principales y Deployments

### 1. API REST en Rust (weather-api-rust)
**Descripción**: Punto de entrada al sistema que recibe peticiones HTTP con información sobre el clima.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: weather-api-rust
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: weather-api-rust
  replicas: 1  # Se puede escalar a 2 o más para pruebas
  template:
    metadata:
      labels:
        app: weather-api-rust
    spec:
      containers:
      - name: weather-api-rust
        image: $HARBOR_IP/weather-tweets/weather-api-rust:v1
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "200m"
```

**Funcionamiento**: 
- Recibe mensajes JSON con información de clima (país, tipo de clima, descripción)
- Proporciona un endpoint `/input` para recibir datos
- Escala horizontalmente con HPA basado en uso de CPU

### 2. Go API y gRPC Client (go-api)

**Descripción**: Puente entre la API REST y los servicios gRPC para message brokers.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: go-api
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: go-api
  replicas: 1
  template:
    metadata:
      labels:
        app: go-api
    spec:
      containers:
      - name: go-api
        image: $HARBOR_IP/weather-tweets/go-api:v1
        ports:
        - containerPort: 8080
        env:
        - name: RABBIT_GRPC_ADDR
          value: "rabbit-writer-service:50051"
        - name: KAFKA_GRPC_ADDR
          value: "kafka-writer-service:50052"
```

**Funcionamiento**:
- Recibe peticiones de la API Rust
- Actúa como cliente gRPC hacia los servicios writer
- Distribuye cada mensaje a ambos message brokers (RabbitMQ y Kafka)


### 3. RabbitMQ Writer (rabbit-writer)

**Descripción**: Servicio gRPC que publica mensajes en RabbitMQ.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbit-writer
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: rabbit-writer
  replicas: 1
  template:
    metadata:
      labels:
        app: rabbit-writer
    spec:
      containers:
      - name: rabbit-writer
        image: $HARBOR_IP/weather-tweets/rabbit-writer:v1
        ports:
        - containerPort: 50051
        env:
        - name: RABBITMQ_URI
          value: "amqp://guest:guest@rabbitmq:5672/"
        - name: RABBITMQ_QUEUE
          value: "message"
```

**Funcionamiento**:
- Expone un servicio gRPC con método `PublishToRabbitMQ`
- Recibe datos y los publica en la cola "message" de RabbitMQ
- Maneja reconexiones a RabbitMQ en caso de fallos

### 4. Kafka Writer (kafka-writer)

**Descripción**: Servicio gRPC que publica mensajes en Kafka.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-writer
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: kafka-writer
  replicas: 1
  template:
    metadata:
      labels:
        app: kafka-writer
    spec:
      containers:
      - name: kafka-writer
        image: $HARBOR_IP/weather-tweets/kafka-writer:v1
        ports:
        - containerPort: 50052
        env:
        - name: KAFKA_BROKER
          value: "kafka:9092"
        - name: KAFKA_TOPIC
          value: "message"
```

**Funcionamiento**:
- Expone un servicio gRPC con método `PublishToKafka`
- Recibe datos y los publica en el tópico "message" de Kafka
- Utiliza el país como clave para particionamiento