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

### 5. RabbitMQ Consumer (rabbit-consumer)

**Descripción**: Consume mensajes de RabbitMQ y almacena estadísticas en Valkey.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbit-consumer
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: rabbit-consumer
  replicas: 1
  template:
    metadata:
      labels:
        app: rabbit-consumer
    spec:
      containers:
      - name: rabbit-consumer
        image: $HARBOR_IP/weather-tweets/rabbit-consumer:v1
        env:
        - name: RABBITMQ_URI
          value: "amqp://guest:guest@rabbitmq:5672/"
        - name: RABBITMQ_QUEUE
          value: "message"
        - name: VALKEY_ADDR
          value: "valkey:6379"
```

**Funcionamiento**:
- Consume mensajes de la cola "message" de RabbitMQ
- Utiliza múltiples goroutines (worker pool pattern) para procesamiento concurrente
- Almacena estadísticas en Valkey


### 6. Kafka Consumer (kafka-consumer)

**Descripción**: Consume mensajes de Kafka y almacena estadísticas en Redis.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-consumer
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: kafka-consumer
  replicas: 1
  template:
    metadata:
      labels:
        app: kafka-consumer
    spec:
      containers:
      - name: kafka-consumer
        image: $HARBOR_IP/weather-tweets/kafka-consumer:v1
        env:
        - name: KAFKA_BROKER
          value: "kafka:9092"
        - name: KAFKA_TOPIC
          value: "message"
        - name: KAFKA_GROUP_ID
          value: "weather-consumer-group"
        - name: REDIS_ADDR
          value: "redis:6379"
```

**Funcionamiento**:
- Consume mensajes del tópico "message" de Kafka
- Utiliza un pool de workers (goroutines) para procesamiento concurrente
- Almacena estadísticas en Redis

### 7. RabbitMQ

**Descripción**: Message broker para publicación-suscripción basado en colas.

**Deployment**: Utilizando Helm Chart de Bitnami

**Funcionamiento**:
- Almacena mensajes en colas durables
- Proporciona garantías de entrega con ACK/NACK
- Ofrece modelo push a los consumidores

### 8. Kafka

**Descripción**: Plataforma de streaming distribuida para publicación-suscripción.

**Deployment**: Utilizando Helm Chart de Bitnami

**Funcionamiento**:
- Almacena mensajes en tópicos particionados
- Retiene mensajes incluso después de ser consumidos
- Ofrece modelo pull para los consumidores

### 9. Redis y Valkey

**Descripción**: Bases de datos en memoria para almacenar estadísticas.

**Deployment**:
```yaml
# Redis StatefulSet
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
  namespace: weather-tweets
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7.0
        args: ["--appendonly", "yes"]
```

### 10. Grafana

**Descripción**: Plataforma de visualización para estadísticas.

**Deployment**: Utilizando Helm Chart de Grafana

## 11. Locust

**Descripción**: Herramienta para pruebas de carga.

**Deployment**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: locust
  namespace: weather-tweets
spec:
  selector:
    matchLabels:
      app: locust
  replicas: 1
  template:
    metadata:
      labels:
        app: locust
    spec:
      containers:
      - name: locust
        image: $HARBOR_IP/weather-tweets/locust:v1
        env:
        - name: TARGET_HOST
          value: "http://$INGRESS_IP.nip.io"
```

## Respuestas

### 1. ¿Cómo funciona Kafka?
Kafka funciona como un sistema de mensajería que organiza los datos en particiones. Los producers envían mensajes a estos tópicos, y Kafka los almacena de forma persistente en discos. Los consumers leen estos mensajes cuando lo solicitan y pueden procesar datos a su propio ritmo. 

### 2. ¿Cómo difiere Valkey de Redis?
Valkey es un fork de Redis con diferencias principalmente en la licencia. Valkey usa la licencia BSD-3-Clause (más permisiva) mientras Redis usa RSAL para algunos módulos. Valkey tiene un enfoque más comunitario en su desarrollo, mientras Redis está más controlado por Redis Ltd. Técnicamente son muy similares.

### 3. ¿Es mejor gRPC que HTTP?
La comparación entre gRPC y HTTP REST no tiene una respuesta definitiva, ya que cada tecnología tiene ventajas en diferentes contextos. depende del caso de uso. gRPC ofrece mejor rendimiento gracias a Protocol Buffers (serialización binaria) y HTTP/2 (multiplexación), proporciona contratos de API formales con archivos .proto, y permite streaming bidireccional. HTTP REST es más universal, simple de implementar y depurar, funciona directamente en navegadores, y tiene mejor soporte.

### 4. ¿Hubo una mejora al utilizar dos réplicas en los deployments de API REST y gRPC?
Sí, se observaron mejoras significativas con dos réplicas. El rendimiento aumentó aproximadamente las  solicitudes por segundo. La latencia bajo y la carga se redujo. Se mejoro la distribución de recursos.

### 5. Para los consumidores, ¿Qué utilizó y por qué?
Utilizamos goroutines de Go porque son extremadamente livianas comparadas con hilos tradicionales. Esto permitió procesar más de 1,000 mensajes por segundo con uso estable de recursos, además de proporcionar recuperación automática ante fallos.