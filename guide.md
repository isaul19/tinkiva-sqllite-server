Sí. Eso que buscas encaja perfectamente con SQLite, y de hecho yo evitaría sqld si quieres el mínimo
consumo posible.

La idea sería tener un único proceso Rust/Axum activo y cientos de archivos SQLite que solo se abren
cuando alguien los usa.

EC2 │ ├── tinkiva-db-server ← único proceso activo │ Rust + Axum │ └── /data/databases/ ├──
empresa-001.db 💤 ├── empresa-002.db 💤 ├── empresa-003.db 💤 ├── empresa-004.db 💤 └── ...

Una base SQLite no es un proceso. Cuando nadie la tiene abierta, es literalmente un archivo en EBS:

empresa-001.db

No tiene:

CPU: 0 thread propio: 0 proceso propio: 0 RAM dedicada: 0

Puede quedar algo en la caché de archivos del kernel Linux, pero esa memoria es recuperable por el
sistema cuando la necesita.

Tu servidor podría escuchar, por ejemplo, en:

:7000

y recibir:

GET /v1/db/litos/cats POST /v1/db/litos/cats

GET /v1/db/codevia/products POST /v1/db/codevia/products

Internamente:

request ↓ tenant = "litos" ↓ /data/databases/litos.db ↓ ¿está abierta? │ ├─ sí → reutilizar conexión
│ └─ no → abrir SQLite ↓ ejecutar ↓ responder Y la parte que buscas: volverla a “dormir”

Podemos hacer un LRU de conexiones.

Por ejemplo:

Máximo DB abiertas: 20 Timeout inactivo: 5 minutos

Supón que tienes 500 empresas, pero solo 4 están usando el sistema ahora:

500 archivos SQLite

empresa-001.db 💤 empresa-002.db 💤 empresa-003.db 🟢 abierta empresa-004.db 💤 empresa-005.db 🟢
abierta ... empresa-173.db 🟢 abierta ... empresa-411.db 🟢 abierta empresa-500.db 💤

En RAM solo tendrías aproximadamente:

Tinkiva DB Server

- 4 conexiones SQLite activas
- cache de esas DB

Las otras 496 DB no estarían consumiendo recursos propios.

Después de, digamos, cinco minutos:

empresa-003.db sin requests 1 min ↓ cerrar conexiones ↓ SQLite checkpoint si corresponde ↓ remover
del pool ↓ 💤 WAL sigue funcionando perfectamente

Cuando abrimos la DB por primera vez podemos ejecutar/configurar:

PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON; PRAGMA
busy_timeout = 1000;

Si alguien hace una peticion se resetea el contador

Eso queda configurado por base de datos.

Mientras está activa podrías tener:

litos.db litos.db-wal litos.db-shm

y cuando ya no la utilizamos:

cerramos las conexiones ↓ checkpoint ↓ la DB queda simplemente almacenada

No necesitas un daemon SQLite por cada una.

Yo construiría exactamente esto en Rust

Algo conceptualmente así:

struct DatabaseManager { databases: HashMap<String, DatabaseHandle>, max_open: usize, idle_timeout:
Duration, }

Cada handle:

struct DatabaseHandle { connection: Connection, last_used: Instant, }

Y:

                 Axum :7000
                     │
                     ▼
             DatabaseManager
                     │
           ┌─────────┴──────────┐
           │                    │
       Active DBs            Sleeping
           │                    │
      ┌────┼────┐         ┌─────┼─────┐
      ▼    ▼    ▼         ▼     ▼     ▼
     A.db B.db C.db       D.db  E.db  F.db
      ↑    ↑    ↑
    RAM/cache              0 runtime

Un background task cada minuto podría hacer:

for database in open_databases:

    if now - last_used > 5 min:
        checkpoint
        close
        remove_from_pool

Esto escala de una forma muy interesante

Podrías tener:

10 DB 100 DB 1,000 DB 10,000 DB

y eso no significa tener 10,000 bases cargadas en RAM.

Podrías definir:

10,000 databases almacenadas 50 máximo abiertas simultáneamente

Por ejemplo:

[database] directory = "/var/lib/tinkivadb" journal_mode = "wal"

[pool] max_open_databases = 50 idle_timeout = "5m" connections_per_database = 2

Y aunque tengas 10,000 clientes:

disco: 10,000 .db

RAM: solo las DB utilizadas recientemente Incluso podríamos llevarlo más lejos

Si después de 30 días una empresa no usa su DB:

HOT EBS ↓ 30 días inactiva ↓ COLD S3

Entonces:

empresa-001.db → EBS empresa-002.db → EBS empresa-003.db → S3 🧊 empresa-004.db → S3 🧊

Cuando vuelve el cliente:

request empresa-003 ↓ DB no está local ↓ descargar S3 ↓ abrir SQLite ↓ HOT

Eso ya empezaría a parecerse muchísimo a una plataforma serverless:

             TinkivaDB :7000

                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼

    HOT DB       HOT DB       HOT DB
      RAM          RAM          RAM

────────────────────────────────────

      SQLite files en EBS
             💤 💤 💤

────────────────────────────────────

       DB archivadas en S3
             🧊 🧊 🧊

Y eso es exactamente lo que entiendo que buscas

No:

# 100 databases

100 procesos

sino:

1 proceso Rust │ ├── abre DB cuando se usa ├── mantiene unas pocas calientes ├── cierra DB inactivas
└── miles de SQLite dormidas

Eso sí me parece más interesante que instalar sqld, porque es bastante sencillo de construir,
extremadamente ligero y podemos controlar exactamente el comportamiento de memoria que quieres.

En una t4g.small de 2 GB, una arquitectura así podría alojar muchísimas bases SQLite pequeñas,
siempre que solo una fracción esté activa simultáneamente.
