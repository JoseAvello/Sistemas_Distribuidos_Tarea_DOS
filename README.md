# Tarea 2 - Sistemas Distribuidos

## Descripción

Esta tarea consiste en extender la solución desarrollada en la Tarea 1 incorporando Apache Kafka para el procesamiento asíncrono de consultas.

El sistema utiliza Redis como caché para mejorar los tiempos de respuesta y Kafka para desacoplar la generación y el procesamiento de consultas. Además, se implementó un mecanismo de reintentos y una cola DLQ para manejar errores.

---

## Tecnologías utilizadas

* Python
* Flask
* Docker
* Docker Compose
* Redis
* Apache Kafka
* Zookeeper

---

## Arquitectura

```text
Generador de Tráfico
        |
        v
      Kafka
        |
        v
Kafka Consumer
        |
        v
 Cache Service
    |        |
    v        v
 Redis   Generador de Respuestas

Errores
   |
   v
Retry Topic
   |
   v
Kafka Retry Consumer
   |
   +--> Recuperación
   |
   +--> DLQ
```

---

## Servicios

### Generador de Tráfico

Genera consultas y las publica en Kafka.

### Kafka Consumer

Consume las consultas desde Kafka y las envía al sistema para su procesamiento.

### Kafka Retry Consumer

Reprocesa las consultas que fallaron durante el primer intento.

### Cache Service

Gestiona el acceso a Redis y determina si una consulta corresponde a un hit o un miss.

### Generador de Respuestas

Obtiene la información desde el conjunto de datos cuando la respuesta no se encuentra en caché.

### Servicio de Métricas

Registra estadísticas relacionadas con el funcionamiento del sistema.

---

## Tópicos Kafka

- `consultas`: tópico principal donde se publican las consultas generadas.
- `retry_topic`: tópico utilizado para reenviar consultas que fallaron durante su procesamiento.
- `dlq`: cola de mensajes muertos (Dead Letter Queue) para consultas que no pudieron recuperarse después de varios intentos.
---

## Ejecución

Levantar todos los servicios:

```bash
docker compose up --build
```

Ver contenedores en ejecución:

```bash
docker compose ps
```

Detener servicios:

```bash
docker compose down -v
```

---

## Métricas

Las métricas pueden consultarse en:

```text
http://localhost:8001/metricas
```

Se registran métricas como:

* Hit Rate
* Miss Rate
* Retry Rate
* Recovery Rate
* DLQ Rate
* Latencias de procesamiento

---

## Manejo de errores

Cuando una consulta falla:

1. Se envía al tópico `consultas_retry`.
2. Se intenta reprocesar.
3. Si se recupera correctamente se registra como recovery.
4. Si supera el número máximo de intentos se envía a `consultas_dlq`.

---

## Integrantes

* Jose Avello
* Alvaro Valdebenito

---

## Asignatura

Sistemas Distribuidos
