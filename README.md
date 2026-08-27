# TinkivaDatabase

TinkivaDatabase es un servicio pequeño escrito en Rust que expone múltiples archivos SQLite aislados
a través de un único proceso HTTP. Las bases de datos se abren cuando se solicitan, permanecen activas
durante un periodo configurable y se cierran con un checkpoint de WAL cuando quedan inactivas o cuando
se necesita espacio por la capacidad LRU.

Está pensado para cargas de trabajo con muchos tenants pequeños, de los cuales solo una fracción está
activa al mismo tiempo. No es una base de datos SQL distribuida: ejecute un único servicio escritor sobre
almacenamiento persistente local y escale distribuyendo los tenants entre varias instancias.

## Inicio rápido

```bash
cargo run --release -- --config config.example.toml
```

El listener predeterminado es `127.0.0.1:7000`. Una base de datos llamada `acme` se crea de forma
perezosa en la primera solicitud, en `data/databases/acme.db`.

```bash
# Crear una tabla
curl -X POST http://127.0.0.1:7000/v1/db/acme/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{"sql":"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)"}'

# Insertar de forma segura usando parámetros posicionales
curl -X POST http://127.0.0.1:7000/v1/db/acme/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{"sql":"INSERT INTO products(name, price) VALUES (?, ?)","params":["Keyboard",49.9]}'

# Consultar
curl -X POST http://127.0.0.1:7000/v1/db/acme/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{"sql":"SELECT * FROM products WHERE price < ?","params":[100]}'
```

## API HTTP

Todos los cuerpos de solicitud son JSON. Los parámetros son posicionales y corresponden a los
marcadores `?` de SQLite. Los arrays y objetos se almacenan como texto JSON. Los resultados BLOB se
representan como `{"$blob":"<base64>"}`.

`/query` se ejecuta mediante conexiones de solo lectura, por lo que rechaza las sentencias que escriben;
estas deben enviarse a `/execute` o `/batch`. Cualquier solicitud puede incluir `"format": "arrays"`
para recibir las filas como arrays posicionales, en lugar de repetir los nombres de las columnas en
cada fila. En conjuntos de resultados anchos esto reduce considerablemente el tamaño de la respuesta.

| Método | Ruta | Propósito |
| --- | --- | --- |
| `GET` | `/health` | Comprobación de disponibilidad sin autenticación |
| `POST` | `/v1/db/{database}/query` | Ejecutar una consulta y devolver filas |
| `POST` | `/v1/db/{database}/execute` | Ejecutar una sentencia DDL/DML |
| `POST` | `/v1/db/{database}/batch` | Ejecutar varias sentencias DDL/DML de forma atómica |
| `GET` | `/v1/admin/stats` | Informar de las bases abiertas, leases activos y capacidad |
| `GET` | `/v1/admin/metrics` | Métricas Prometheus: latencia por ruta, espera de admisión y solicitudes rechazadas |

Cuerpo de una operación por lotes:

```json
{
  "statements": [
    { "sql": "INSERT INTO products(name, price) VALUES (?, ?)", "params": ["Mouse", 20] },
    { "sql": "UPDATE products SET price = ? WHERE name = ?", "params": [18, "Mouse"] }
  ]
}
```

El lote se revierte automáticamente si falla alguna sentencia. Cada sentencia informa de las filas
devueltas junto con `rows_affected` y `last_insert_rowid`, por lo que una secuencia de lectura,
modificación y lectura puede realizarse en una sola solicitud y transacción. Los resultados se detienen
en `max_result_rows` y devuelven `"truncated": true` cuando existen más filas.

Los nombres de las bases de datos aceptan entre 1 y 64 caracteres ASCII: letras, dígitos, `-` y `_`.
Deben comenzar por una letra o un dígito. Esto evita el recorrido de rutas y hace que cada tenant se
corresponda exactamente con un archivo.

## Configuración

Copie `config.example.toml` y páselo mediante `--config`. Estas variables de entorno sobrescriben los
valores que se cambian con mayor frecuencia:

- `TINKIVA_CONFIG`
- `TINKIVA_BIND`
- `TINKIVA_AUTH_TOKEN`
- `TINKIVA_DATABASE_DIR`
- `TINKIVA_MAX_OPEN_DATABASES`
- `TINKIVA_IDLE_TIMEOUT_SECONDS`
- `TINKIVA_CLEANUP_INTERVAL_SECONDS`
- `TINKIVA_CHECKPOINT_INTERVAL_SECONDS`
- `TINKIVA_READER_CONNECTIONS`
- `TINKIVA_BUSY_TIMEOUT_MS`
- `TINKIVA_ACQUIRE_TIMEOUT_SECONDS`
- `TINKIVA_MAX_RESULT_ROWS`
- `TINKIVA_WRITER_CACHE_SIZE_KB`
- `TINKIVA_READER_CACHE_SIZE_KB`
- `TINKIVA_CACHE_SIZE_KB` (sobrescritura heredada para ambos roles)
- `TINKIVA_STATEMENT_CACHE_CAPACITY`
- `TINKIVA_MMAP_SIZE_MB`
- `TINKIVA_MAX_CONCURRENT_REQUESTS`
- `TINKIVA_MAX_CONCURRENT_REQUESTS_PER_DATABASE`
- `TINKIVA_ADMISSION_TIMEOUT_MS`
- `RUST_LOG` (por ejemplo, `tinkiva_database=debug,tower_http=info`)

Use TLS en un proxy inverso y configure un token bearer robusto antes de exponer el servicio a una red.
El token único configurado es adecuado para un despliegue privado entre servicios; las credenciales por
tenant y la aplicación de políticas no forman parte de este MVP.

## Modelo operativo

- SQLite se ejecuta en modo WAL, con `synchronous=NORMAL`, claves foráneas activadas y un tiempo de espera para bases ocupadas.
- Cada base de datos obtiene una conexión escritora y un pool de lectores separado, de modo que una escritura nunca bloquea una lectura. El pool de lectores es perezoso: una base que solo recibe escrituras no abre conexiones de lectura.
- Las cachés de páginas del escritor y de los lectores tienen presupuestos separados porque sus conjuntos de trabajo son distintos. La memoria residente aproximada por base activa es `writer_cache_size_kb + readers × reader_cache_size_kb`. La ventana `mmap_size_mb` está respaldada por el archivo y puede desalojarse, por lo que no añade memoria privada.
- La caché de sentencias preparadas está limitada por conexión. El SQL parametrizado reutiliza un número reducido de entradas; el SQL cuyo texto cambia continuamente se desaloja en lugar de acumularse entre tenants.
- Un contador de leases protege las solicitudes en curso frente a la limpieza por inactividad y el desalojo LRU.
- Cuando se alcanza la capacidad, se realiza un checkpoint y se cierra la base inactiva menos usada recientemente.
- Si todas las bases abiertas están activas, el nuevo tenant recibe HTTP `503 capacity_busy`.
- Las solicitudes adquieren un slot por base y otro global para el proceso. Después de `admission_timeout_ms`, la solicitud se rechaza con HTTP `429 overloaded` y una cabecera `Retry-After`, en lugar de añadir latencia mediante una cola.
- Los checkpoints de WAL se ejecutan con un temporizador en segundo plano, por lo que ninguna solicitud asume su coste bajo carga normal; `wal_size_limit_mb` es el umbral a partir del cual una solicitud hará un checkpoint como último recurso.
- El apagado ordenado realiza checkpoints y cierra todos los pools administrados.
- Los archivos de base de datos permanecen en el almacenamiento persistente local mientras están inactivos; no existe una capa remota de almacenamiento en frío.

Realice copias de seguridad de SQLite mediante un método de snapshot o backup compatible con SQLite.
Copiar un archivo `.db` activo sin su WAL o sin coordinar un checkpoint puede producir una copia
inconsistente.

## Desarrollo

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Consulte [guide.md](guide.md) para conocer la arquitectura y los límites de escalabilidad. El
rendimiento multi-tenant medido, la memoria y la latencia se documentan en [BENCHMARKS.md](BENCHMARKS.md),
incluida una comparación A/B con la compilación anterior usando un cliente.

## Despliegue

Para un despliegue en contenedor, configure un token e inicie el stack de Compose incluido:

```bash
export TINKIVA_AUTH_TOKEN="replace-with-a-long-random-value"
docker compose up -d --build
```

El volumen con nombre contiene los archivos de base de datos persistentes, mientras que el contenedor
se ejecuta como un usuario sin privilegios y publica el servicio en el loopback del host. Coloque un
proxy inverso TLS delante si los clientes remotos necesitan acceso.

Para una máquina virtual Linux pequeña sin contenedores, compile el binario de release, cree un usuario
de sistema `tinkiva`, coloque la configuración en `/etc/tinkivadb`, los datos en
`/var/lib/tinkivadb/databases` e instale la unidad de `deploy/tinkiva-database.service`. La unidad
restringe el acceso al sistema de archivos al directorio de datos y concede 30 segundos para realizar
un apagado ordenado con checkpoint de los pools.
