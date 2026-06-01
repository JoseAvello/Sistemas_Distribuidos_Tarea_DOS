from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import os
import time
import signal


MAX_RETRIES = 3

URL_CACHE = os.environ.get(
    "CACHE_URL",
    "http://cache_service:8003"
)

URL_METRICAS = os.environ.get(
    "METRICAS_URL",
    "http://metricas:8001"
)

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "kafka:9092"
)

TOPIC_RETRY = "retry_topic"
TOPIC_DLQ = "dlq"

running = True


def manejar_salida(signum, frame):
    global running
    print("Señal de apagado recibida en kafka_retry_consumer...", flush=True)
    running = False


signal.signal(signal.SIGTERM, manejar_salida)
signal.signal(signal.SIGINT, manejar_salida)


def crear_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_RETRY,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                group_id="grupo_retry",
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                consumer_timeout_ms=1000
            )

            print("Retry Consumer conectado a Kafka", flush=True)
            return consumer

        except Exception as e:
            print(f"Kafka Consumer no disponible: {e}", flush=True)
            time.sleep(5)


def crear_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )

            print("Producer conectado a Kafka", flush=True)
            return producer

        except Exception as e:
            print(f"Kafka Producer no disponible: {e}", flush=True)
            time.sleep(5)


def registrar_metrica(tipo):
    try:
        requests.post(
            f"{URL_METRICAS}/registrar",
            json={"tipo": tipo},
            timeout=2
        )
    except Exception:
        pass


def procesar_consulta(consulta):
    respuesta = requests.post(
        f"{URL_CACHE}/consulta",
        json=consulta,
        timeout=30
    )

    respuesta.raise_for_status()

    return respuesta.json()


def enviar_a_dlq(producer, consulta, error):
    consulta["last_error"] = str(error)

    producer.send(
        TOPIC_DLQ,
        consulta
    )

    producer.flush()

    registrar_metrica("dlq")

    print(
        f"Consulta enviada a DLQ: {consulta.get('id')}",
        flush=True
    )


def reenviar_a_retry(producer, consulta, error):
    consulta["retry_count"] = consulta.get("retry_count", 0) + 1
    consulta["last_error"] = str(error)

    producer.send(
        TOPIC_RETRY,
        consulta
    )

    producer.flush()

    registrar_metrica("retry")

    print(
        f"Consulta reenviada a retry. Intento {consulta['retry_count']}: {consulta.get('id')}",
        flush=True
    )


def main():
    consumer = crear_consumer()
    producer = crear_producer()

    print("Retry Consumer iniciado...", flush=True)

    try:
        while running:
            mensajes = consumer.poll(timeout_ms=1000)

            for topic_partition, records in mensajes.items():
                for mensaje in records:
                    consulta = mensaje.value

                    print(
                        f"Retry recibido: {consulta.get('id')}",
                        flush=True
                    )

                    time.sleep(2)

                    try:
                        procesar_consulta(consulta)

                        print(
                            f"Consulta recuperada correctamente: {consulta.get('id')}",
                            flush=True
                        )

                        registrar_metrica("recovery")

                    except Exception as e:
                        retries = consulta.get("retry_count", 0)

                        if retries >= MAX_RETRIES:
                            enviar_a_dlq(
                                producer,
                                consulta,
                                e
                            )
                        else:
                            reenviar_a_retry(
                                producer,
                                consulta,
                                e
                            )

                    finally:
                        consumer.commit()

    finally:
        print("Cerrando kafka_retry_consumer...", flush=True)

        try:
            consumer.close()
        except Exception:
            pass

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        print("kafka_retry_consumer cerrado correctamente.", flush=True)


if __name__ == "__main__":
    main()