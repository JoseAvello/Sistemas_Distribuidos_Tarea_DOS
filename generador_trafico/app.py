import os
import time
import random
import math
import requests
import json
import uuid

from kafka import KafkaProducer

URL_METRICAS = os.environ.get(
    "METRICAS_URL",
    "http://metricas:8001"
)

DISTRIBUCION = os.environ.get(
    "DISTRIBUCION",
    "zipf"
)

TOTAL_CONSULTAS = int(
    os.environ.get(
        "TOTAL_CONSULTAS",
        500
    )
)

CONSULTAS_POR_SEGUNDO = float(
    os.environ.get(
        "CONSULTAS_POR_SEGUNDO",
        10
    )
)


producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Conectado a Kafka")
    except Exception as e:
        print(f"Kafka no disponible: {e}")
        time.sleep(5)

ZONAS_IDS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

TIPOS_CONSULTA = [
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5"
]

VALORES_CONFIANZA = [
    0.0,
    0.5,
    0.7,
    0.9
]

VALORES_BINS = [
    5,
    10
]


def muestrear_zipf(n_elementos, s=1.5):

    pesos = [
        1.0 / (i ** s)
        for i in range(1, n_elementos + 1)
    ]

    suma = sum(pesos)

    pesos_norm = [
        p / suma
        for p in pesos
    ]

    r = random.random()

    acumulado = 0

    for i, p in enumerate(pesos_norm):

        acumulado += p

        if r <= acumulado:
            return i

    return n_elementos - 1


def generar_consulta_zipf():

    idx_zona = muestrear_zipf(
        len(ZONAS_IDS)
    )

    idx_tipo = muestrear_zipf(
        len(TIPOS_CONSULTA)
    )

    zona_id = ZONAS_IDS[idx_zona]

    tipo = TIPOS_CONSULTA[idx_tipo]

    confianza_min = VALORES_CONFIANZA[
        muestrear_zipf(
            len(VALORES_CONFIANZA)
        )
    ]

    bins = VALORES_BINS[0]

    consulta = {
        "tipo": tipo,
        "zona_id": zona_id,
        "confianza_min": confianza_min,
        "bins": bins
    }

    if tipo == "Q4":

        zonas_restantes = [
            z for z in ZONAS_IDS
            if z != zona_id
        ]

        consulta["zona_b"] = zonas_restantes[
            muestrear_zipf(
                len(zonas_restantes)
            )
        ]

    return consulta


def generar_consulta_uniforme():

    zona_id = random.choice(
        ZONAS_IDS
    )

    tipo = random.choice(
        TIPOS_CONSULTA
    )

    confianza_min = random.choice(
        VALORES_CONFIANZA
    )

    bins = random.choice(
        VALORES_BINS
    )

    consulta = {
        "tipo": tipo,
        "zona_id": zona_id,
        "confianza_min": confianza_min,
        "bins": bins
    }

    if tipo == "Q4":

        zonas_restantes = [
            z for z in ZONAS_IDS
            if z != zona_id
        ]

        consulta["zona_b"] = random.choice(
            zonas_restantes
        )

    return consulta


def enviar_consulta(consulta):

    try:

        consulta["id"] = str(
            uuid.uuid4()
        )

        consulta["timestamp"] = time.time()

        consulta["retry_count"] = 0

        producer.send(
            "consultas",
            consulta
        )

        return True

    except Exception as e:

        print(
            f"ERROR enviando a Kafka: {e}",
            flush=True
        )

        return False


def esperar_kafka(reintentos=30):
    for i in range(reintentos):
        try:
            if producer.bootstrap_connected():
                print("Kafka disponible.", flush=True)
                return True
        except Exception:
            pass

        print(
            f"Esperando Kafka... ({i+1}/{reintentos})",
            flush=True
        )

        time.sleep(3)

    return False


def main():

    print(
        "=== Generador de Tráfico ===",
        flush=True
    )

    print(
        f"Distribución: {DISTRIBUCION}",
        flush=True
    )

    print(
        f"Total consultas: {TOTAL_CONSULTAS}",
        flush=True
    )

    print(
        f"Tasa: {CONSULTAS_POR_SEGUNDO} consultas/seg",
        flush=True
    )

    if not esperar_kafka():

        print(
            "Kafka no disponible.",
            flush=True
        )

        return

    try:

        requests.post(
            f"{URL_METRICAS}/reiniciar",
            timeout=5
        )

        print(
            "Métricas reiniciadas.",
            flush=True
        )

    except Exception:

        pass

    intervalo = 1.0 / CONSULTAS_POR_SEGUNDO

    enviadas = 0
    errores = 0

    print(
        "\nIniciando generación de tráfico...\n",
        flush=True
    )

    for i in range(TOTAL_CONSULTAS):

        if DISTRIBUCION == "zipf":

            consulta = generar_consulta_zipf()

        else:

            consulta = generar_consulta_uniforme()

        ok = enviar_consulta(
            consulta
        )

        if ok:
            enviadas += 1
        else:
            errores += 1

        if (i + 1) % 50 == 0:

            print(
                f"[{i+1}/{TOTAL_CONSULTAS}] "
                f"enviadas={enviadas} "
                f"errores={errores}",
                flush=True
            )

        time.sleep(intervalo)

    producer.flush()

    print(
        "\n=== Resumen Final ===",
        flush=True
    )

    print(
        f"Consultas enviadas: {enviadas}",
        flush=True
    )

    print(
        f"Errores: {errores}",
        flush=True
    )

    try:

        time.sleep(120)

        resp = requests.get(
            f"{URL_METRICAS}/metricas",
            timeout=5
        )

        metricas = resp.json()

        print(
            "\nMétricas del sistema:",
            flush=True
        )

        print(
            json.dumps(
                metricas,
                indent=2
            ),
            flush=True
        )

    except Exception as e:

        print(
            f"No se pudieron obtener métricas: {e}",
            flush=True
        )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
