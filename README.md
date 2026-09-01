# Kafka Broker Docker

Configuración de un broker de Apache Kafka en modo **KRaft** (sin Zookeeper), listo para desarrollo local, junto con **Kafka UI** para visualizar tópicos, mensajes y el estado del clúster.

> **Referencia oficial**: esta configuración está basada en la documentación oficial de Apache Kafka y su ejemplo de docker-compose:
> - https://kafka.apache.org/43/getting-started/docker/
> - https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) instalado
- [Docker Compose](https://docs.docker.com/compose/install/) instalado (viene incluido con Docker Desktop)

## Cómo levantar el contenedor

1. Cloná el repositorio:

   ```bash
   git clone https://github.com/JulianMorenoSoftware/kafka-broker-docker.git
   cd kafka-broker-docker
   ```

2. Levantá los servicios en segundo plano:

   ```bash
   docker compose up -d
   ```

3. Verificá que los contenedores estén corriendo:

   ```bash
   docker compose ps
   ```

4. Revisá los logs del broker (útil para confirmar que arrancó correctamente):

   ```bash
   docker compose logs -f broker
   ```

5. Accedé a **Kafka UI** desde el navegador:

   ```
   http://localhost:9080
   ```

### Comandos útiles

Crear un tópico de prueba:

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Listar tópicos existentes:

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Producir mensajes desde consola:

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh \
  --topic test-topic \
  --bootstrap-server localhost:9092
```

Consumir mensajes desde consola:

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Detener y eliminar los contenedores:

```bash
docker compose down
```

Detener y eliminar los contenedores **junto con los volúmenes** (borra los datos del clúster):

```bash
docker compose down -v
```

## Explicación de las variables de entorno

### Servicio `broker`

| Variable | Descripción |
|---|---|
| `KAFKA_NODE_ID` | Identificador único del nodo dentro del clúster Kafka. En KRaft, cada nodo (broker y/o controller) necesita un ID numérico único. |
| `CLUSTER_ID` | Identificador único del clúster, codificado en Base64. Se genera una sola vez (por ejemplo con `kafka-storage.sh random-uuid`) y debe ser el mismo en todos los nodos del clúster. |
| `KAFKA_PROCESS_ROLES` | Define qué rol(es) cumple este nodo. `broker,controller` significa que el mismo nodo actúa como **broker** (maneja tópicos y datos) y como **controller** (gestiona metadatos del clúster), típico de un setup de un solo nodo en modo KRaft. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | Lista de los nodos que forman el quórum de controllers, en formato `id@host:puerto`. Aquí `1@broker:29093` indica que el nodo con ID `1` es accesible en `broker:29093` para las tareas de controller. |
| `KAFKA_LISTENERS` | Direcciones y puertos internos en los que Kafka escucha conexiones, por nombre de listener. `CONTROLLER://:29093` (comunicación entre controllers), `PLAINTEXT_HOST://:9092` (acceso desde fuera del contenedor) y `PLAINTEXT://:19092` (comunicación interna entre brokers dentro de la red de Docker). |
| `KAFKA_ADVERTISED_LISTENERS` | Direcciones que Kafka anuncia a los clientes para que sepan cómo conectarse. `PLAINTEXT_HOST://${KAFKA_ADVERTISED_HOST:-localhost}:9092` es la dirección que usan los clientes externos — por defecto `localhost`, override vía la variable de entorno `KAFKA_ADVERTISED_HOST` (ver sección "Exponer el broker en tu red" abajo) — y `PLAINTEXT://broker:19092` es la dirección usada por otros servicios dentro de la red Docker (como Kafka UI). |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Mapea cada nombre de listener a un protocolo de seguridad. En este caso todos usan `PLAINTEXT` (sin cifrado ni autenticación), pensado solo para entornos de desarrollo. |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | Indica cuál de los listeners definidos se usa exclusivamente para el tráfico de controller (`CONTROLLER`). |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | Indica qué listener usan los brokers para comunicarse entre sí (`PLAINTEXT`). |
| `KAFKA_LOG_DIRS` | Ruta dentro del contenedor donde Kafka almacena los logs de los tópicos (los datos reales de los mensajes). |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | Factor de replicación del tópico interno `__consumer_offsets` (donde se guardan los offsets de los consumidores). En `1` porque es un solo broker; en producción se recomienda 3. |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | Factor de replicación del tópico interno que almacena el estado de las transacciones. En `1` por ser un único broker. |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | Cantidad mínima de réplicas sincronizadas (in-sync replicas) requeridas para el tópico de estado de transacciones. |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | Tiempo de espera (en ms) antes de que el coordinador de grupo realice el primer rebalanceo de consumidores. En `0` para que el rebalanceo sea inmediato, útil en desarrollo/pruebas. |
| `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR` | Factor de replicación del tópico de estado usado por el mecanismo de "share groups" (consumo compartido) introducido en versiones recientes de Kafka. En `1` por ser un solo broker. |
| `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR` | Mínimo de réplicas sincronizadas requeridas para el tópico de estado de "share groups". |

### Otros parámetros del servicio `broker`

| Parámetro | Descripción |
|---|---|
| `image: apache/kafka:latest` | Imagen oficial de Apache Kafka publicada por el proyecto. |
| `user: appuser` | Usuario no root con el que corre el proceso dentro del contenedor, definido por la imagen oficial. |
| `ports: 9092:9092` | Expone el puerto del listener `PLAINTEXT_HOST` para que clientes fuera de Docker puedan conectarse. |
| `volumes` | Persisten los datos del broker (`kafka-data`), secretos (`kafka-secrets`) y configuración compartida (`kafka-config`) fuera del ciclo de vida del contenedor. |
| `logging` | Limita el tamaño y la cantidad de archivos de log rotados (`max-size: 50m`, `max-file: 3`) para evitar que los logs crezcan indefinidamente en disco. |

### Servicio `kafka-ui`

| Variable | Descripción |
|---|---|
| `KAFKA_CLUSTERS_0_NAME` | Nombre visible del clúster dentro de la interfaz de Kafka UI. |
| `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` | Dirección del broker a la que Kafka UI se conecta, usando el listener interno de Docker (`broker:19092`). |

| Parámetro | Descripción |
|---|---|
| `image: provectuslabs/kafka-ui:latest` | Imagen de la interfaz web de código abierto para administrar clústeres Kafka. |
| `ports: 9080:8080` | Expone la interfaz web en el puerto `9080` del host (el contenedor escucha internamente en el `8080`). |
| `depends_on: broker` | Asegura que el contenedor `broker` inicie antes que `kafka-ui`. |

## Volúmenes

| Volumen | Descripción |
|---|---|
| `kafka-data` | Almacena los datos y logs de los tópicos de Kafka (`/var/lib/kafka/data`). |
| `kafka-secrets` | Almacena certificados y secretos usados por Kafka (`/etc/kafka/secrets`), relevante si en el futuro se habilita SSL/SASL. |
| `kafka-config` | Almacena configuración compartida montada en `/mnt/shared/config`. |

## Exponer el broker en tu red (más allá de `localhost`)

Por defecto, `KAFKA_ADVERTISED_LISTENERS` anuncia `localhost:9092`, lo cual solo permite conectarse desde clientes que corren en la **misma máquina** que el broker. Si necesitás que otro equipo de tu red (o de la red de la empresa) se conecte a este broker, exportá la variable `KAFKA_ADVERTISED_HOST` con la IP real de tu host **antes** de levantar los servicios:

```bash
KAFKA_ADVERTISED_HOST=<ip-real-de-tu-host> docker compose up -d
```

También podés crear un archivo `.env` local (no versionado, agregalo a `.gitignore` si no está) con:

```
KAFKA_ADVERTISED_HOST=<ip-real-de-tu-host>
```

## 🔒 Nota de seguridad

- Esta configuración usa el protocolo `PLAINTEXT` (sin autenticación ni cifrado), pensada **únicamente para entornos de desarrollo local**. No usar en producción sin agregar seguridad (SSL/SASL).
- Este repositorio es **público**. Por eso `KAFKA_ADVERTISED_LISTENERS` usa `localhost` como valor por defecto en vez de una IP real de la red interna: publicar la IP exacta de un broker sin autenticación en un repo público facilita el reconocimiento de red a cualquiera que ya tenga acceso a esa LAN. Nunca commitees un valor real de `KAFKA_ADVERTISED_HOST` en `docker-compose.yml` — usá la variable de entorno o un `.env` local no versionado, como se explica arriba.
