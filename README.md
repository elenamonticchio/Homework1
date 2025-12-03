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
USER_MGR_DB_HOST=...
USER_MGR_PORT=...
USER_MGR_GRPC_HOST=...

# DATA COLLECTOR SERVICE
DATA_COL_DB_HOST=...
DATA_COL_PORT=...

# API GATEWAY
API_GTW_PORT=...

OPEN_SKY_CLIENT_ID=...
OPEN_SKY_CLIENT_SECRET=...

MYSQL_PORT=...
GRPC_PORT=...
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
