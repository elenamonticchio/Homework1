# Progetto Distributed Systems & Big Data

Questo progetto implementa un sistema distribuito basato su **microservizi**, **Kafka**, **MySQL**, **gRPC** e **REST**, con **monitoring white-box tramite Prometheus** e **deployment su Kubernetes**.

Sono disponibili **due modalità di esecuzione**:

- **Docker Compose** (sviluppo locale)
- **Kubernetes (kind)** (deployment completo)

---

## Requisiti

### Per Docker Compose

- Docker
- Docker Compose

### Per Kubernetes

- Docker
- kubectl
- kind (Kubernetes in Docker)

---

## Variabili d’ambiente

Il progetto usa **ConfigMap** e **Secret** in Kubernetes e un file `.env` per Docker Compose.

### Per Docker

Creare un file `.env` nella root del progetto con le seguenti variabili:

```env
# DATA DB
DATA_DB_ROOT_PASSWORD=...
DATA_DB_NAME=...
DATA_DB_USER=...
DATA_DB_PASSWORD=...

# USER DB
USER_DB_ROOT_PASSWORD=...
USER_DB_NAME=...
USER_DB_USER=...
USER_DB_PASSWORD=...

# USER MANAGER
USER_MGR_DB_HOST=user-db
USER_MGR_PORT=5003
USER_MGR_GRPC_HOST=user-manager

# DATA COLLECTOR SERVICE
DATA_COL_DB_HOST=data-db
DATA_COL_PORT=5002

# API GATEWAY
API_GTW_PORT=8000

# OPENSKY CREDENTIALS
OPEN_SKY_CLIENT_ID=...
OPEN_SKY_CLIENT_SECRET=...

SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=...
SMTP_PASS=...
MAIL_FROM=...

# PORTS
MYSQL_PORT=3306
GRPC_PORT=50051
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_PORT=29092

# KAFKA TOPICS
KAFKA_TOPIC_IN=to-alert-system
KAFKA_TOPIC_OUT=to-notifier
KAFKA_TOPIC_FLIGHTS=to-alert-system
```

### Per Kubernetes

Creare un file `app-secrets.yaml` nella cartella k8s-manifests del progetto con la seguente struttura:

```
apiVersion: v1
kind: Secret
metadata:
  name: app-secrets
type: Opaque
stringData:
  # Data DB Secrets
  DATA_DB_ROOT_PASSWORD: "..."
  DATA_DB_USER: "..."
  DATA_DB_PASSWORD: "..."
  # User DB Secrets
  USER_DB_ROOT_PASSWORD: "..."
  USER_DB_USER: "..."
  USER_DB_PASSWORD: "..."
  # Credentials
  OPEN_SKY_CLIENT_ID: "..."
  OPEN_SKY_CLIENT_SECRET: "..."
  SMTP_USER: "..."
  SMTP_PASS: "..."
  MAIL_FROM: "..."

```

---

## Esecuzione con Docker Compose

Modalità consigliata per **sviluppo locale**.

### Build e avvio

```bash
docker compose build
docker compose up
```

Docker Compose gestisce:

- ordine di avvio tramite `depends_on`
- healthcheck sui servizi critici
- persistenza dati con volumi

Configurazione completa in `docker-compose.yaml`

---

## Esecuzione con Kubernetes (kind)

### 1. Creazione cluster kind

```bash
kind create cluster --config kind-config.yaml
```

Il cluster:

- 1 control-plane + 2 worker
- NodePort esposto su `localhost:80`

Configurazione in `kind-config.yaml`


### 2. Build delle immagini Docker

Le immagini sono usate **localmente** dal cluster (`imagePullPolicy: Never`).

```bash
docker build -t user-manager:latest ./user_manager
docker build -t data-collector:latest ./data_collector
docker build -t api-gateway:latest ./api_gateway
docker build -t alert-system:latest ./alert_system
docker build -t alert-notifier:latest ./alert_notifier
```

Caricamento immagini nel cluster kind:

```bash
kind load docker-image user-manager:latest
kind load docker-image data-collector:latest
kind load docker-image api-gateway:latest
kind load docker-image alert-system:latest
kind load docker-image alert-notifier:latest
```

### 3. Deploy su Kubernetes

È possibile applicare **tutti i manifest insieme**, grazie a:

- init containers
- readiness probe
- job di inizializzazione Kafka

```bash
kubectl apply -f k8s-manifests/
```

---

### 4. Accesso ai servizi

**API Gateway**

* Per Docker

  ```
  http://localhost:8000
  ```

* Per Kubernetes

  ```
  http://localhost
  ```


---

## Monitoring con Prometheus

* Per Docker
  ```
  http://localhost:8000/prometheus/
  ```

* Per Kubernetes

  ```
  http://localhost/prometheus/
  ```

---

## Kafka e gestione dei topic

I topic Kafka vengono creati automaticamente tramite **Job Kubernetes**:

- `to-alert-system`
- `to-notifier`


Gestione garantita da:

- Job `kafka-init-topics`
- Init container nei consumer
