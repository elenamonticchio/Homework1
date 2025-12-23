# Build & Deploy – Istruzioni operative

## 1. Prerequisiti specifici del progetto
Per l’esecuzione sono necessari:

- File `.env` con tutte le variabili richieste da `docker-compose.yaml`.
- Accesso a Internet per le chiamate all’API OpenSky.
- Le directory dei servizi devono mantenere la seguente struttura minima:

```
/user_manager
/data_collector
/api_gateway
docker-compose.yaml
```

## 2. Configurazione obbligatoria del file `.env`

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

## 3. Uso dell’API Gateway (porta 8000)

Tutte le chiamate REST passano attraverso l’API Gateway Nginx configurato in `nginx.conf` .

Esempi di endpoint disponibili:

* `/users`, `/users/add`, ... → inoltrati a **user-manager**
* `/interests`, `/flights`, `/users/add-interests`, ... → inoltrati a **data-collector**

Esempio:

```bash
curl http://localhost:8000/users
```

## 4. Note operative utili per questo progetto
- I database richiedono qualche secondo per diventare healthy.
- Le chiamate che usano OpenSky richiedono credenziali valide.
- Tutti gli accessi REST devono passare dall’API Gateway.
