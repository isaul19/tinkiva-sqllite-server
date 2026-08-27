# Guía de arquitectura de TinkivaDatabase

## Objetivo

TinkivaDatabase permite alojar muchas bases SQLite aisladas usando un solo proceso Rust. Cada tenant
se representa con un archivo y solo consume conexiones mientras está activo:

```text
Cliente HTTP
    │
    ▼
Axum :7000 ── autenticación, límites y timeout
    │
    ▼
DatabaseManager ── leases + LRU + limpieza por inactividad
    │
    ├── acme.db       🟢 pool abierto
    ├── litos.db      🟢 pool abierto
    ├── codevia.db    💤 solo archivo
    └── otros/*.db    💤 solo archivos
```

Una base dormida no tiene proceso, hilo, pool ni memoria dedicada. Es un archivo en el volumen local.
La caché de páginas que pueda conservar el sistema operativo es recuperable.

## Flujo de una solicitud

1. La API valida el nombre del tenant para impedir rutas arbitrarias.
2. `DatabaseManager` busca su entrada caliente.
3. Si ya existe, incrementa su lease y reutiliza sus pools.
4. Si no existe, crea o abre `{tenant}.db` con WAL y los pragmas configurados.
5. Si se alcanzó `max_open_databases`, elimina primero la base inactiva menos reciente.
6. La solicitud toma una plaza de admisión de su tenant y otra del proceso; si no la consigue dentro
   de `admission_timeout_ms`, se descarta con `429 overloaded`.
7. La operación SQL se ejecuta con parámetros posicionales: `/query` sobre el pool de lectores,
   `/execute` y `/batch` sobre la conexión de escritura.
8. Al terminar, el lease disminuye y actualiza la hora del último uso.

El registro de bases es un mapa concurrente: encontrar una base caliente no toma ningún cerrojo
global. Solo la admisión de tenants nuevos se serializa, y ese cerrojo nunca se mantiene mientras se
abre una base, de modo que un arranque en frío no bloquea al resto de tenants.

Cada base tiene una sola conexión de escritura, porque SQLite serializa las escrituras sobre el
archivo sin importar cuántas conexiones existan, y un pool aparte de `reader_connections` lectores
con sus propias instantáneas del WAL. Los lectores se abren con `query_only`, así que una escritura
enviada a `/query` es rechazada.

## Dormir una base con seguridad

Una tarea periódica identifica entradas cuyo último uso supera `idle_timeout_seconds`. Solo retira una
entrada si su contador de leases es cero. Después ejecuta:

```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

y cierra el pool. Esto evita cortar consultas en curso y reduce el WAL antes de devolver la base al
estado dormido. Si el checkpoint no puede completarse, el cierre sigue siendo seguro: SQLite conserva
el WAL para recuperarlo al abrir de nuevo.

## Checkpoints del WAL

Una tarea aparte ejecuta `PRAGMA wal_checkpoint(PASSIVE)` sobre cada base abierta cada
`checkpoint_interval_seconds`. PASSIVE nunca bloquea a un lector ni al escritor: si la base está
ocupada, simplemente hace menos trabajo en esa ronda.

El autocheckpoint de SQLite no se desactiva, se eleva hasta `wal_size_limit_mb`. Desactivarlo por
completo deja el WAL sin techo cuando las escrituras superan al temporizador. Con este ajuste una
solicitud solo hace checkpoint como último recurso, y `journal_size_limit` recorta el archivo
después.

## Concurrencia y consistencia

SQLite permite múltiples lectores, pero solo un escritor por archivo. WAL mejora la convivencia entre
lecturas y escritura; no convierte SQLite en una base distribuida. `busy_timeout_ms` hace que una
escritura espere brevemente ante contención en lugar de fallar inmediatamente. Separar el pool de
lectura del escritor traslada esa convivencia al servicio: las escrituras hacen cola en una sola
conexión y las lecturas nunca esperan detrás de ellas.

Para cambios que deban ser atómicos, `/batch` usa una sola transacción y conexión. No hay transacciones
abiertas entre solicitudes HTTP.

El diseño esperado es un único proceso propietario de un directorio local. No se debe montar el mismo
archivo SQLite para escritura concurrente desde varias instancias ni usar un filesystem de red sin
garantías explícitas de bloqueo. Para escalar horizontalmente, asigne cada tenant a un shard estable:

```text
router de tenants
   ├── hash 0..31  → instancia A + volumen A
   ├── hash 32..63 → instancia B + volumen B
   └── hash 64..95 → instancia C + volumen C
```

## Límites de recursos

El límite principal es:

```text
conexiones máximas ≈ max_open_databases × (1 + reader_connections)
```

Con 50 bases abiertas y 2 lectores por base, el techo es aproximadamente 150 conexiones SQLite. El
driver de sqlx dedica un hilo del sistema operativo a cada conexión, así que ese número también es
el techo de hilos: es el límite estructural al escalar hacia miles de tenants calientes.

La memoria residente por base caliente es aproximadamente
`writer_cache_size_kb + lectores × reader_cache_size_kb`. Separar los presupuestos evita duplicar un
caché grande en todos los roles. La ventana `mmap_size_mb` no cuenta como memoria privada: son páginas
del archivo, compartidas y desalojables.
Puede haber miles de archivos dormidos sin crear miles de pools. El número apropiado depende de la
RAM, el patrón de consultas, el page cache y la latencia del volumen.

La API también limita:

- tamaño del body para evitar cargas descontroladas;
- duración de cada solicitud;
- solicitudes simultáneas por base y por proceso, descartando el exceso con `429` en vez de
  absorberlo como latencia;
- filas materializadas por consulta;
- longitud y caracteres del identificador de tenant.

## Seguridad

El MVP ofrece un bearer token global opcional. En producción:

- configure el token mediante secreto/variable de entorno, no dentro de Git;
- termine TLS en un proxy o balanceador;
- mantenga el servicio en una red privada;
- trate el endpoint SQL como acceso total a cada base;
- ejecute el proceso con permisos exclusivos sobre el directorio de datos;
- establezca cuotas y credenciales por tenant antes de ofrecer acceso público directo.

## Backups y recuperación

Los backups deben respetar WAL. Opciones válidas incluyen la API de backup de SQLite, `VACUUM INTO`,
o snapshots de volumen coordinados después de un checkpoint. Se deben probar restauraciones, no solo
la creación de copias.

El apagado normal cierra todos los pools y realiza checkpoints. Ante caída abrupta, WAL permite que
SQLite recupere una transacción consistente al volver a abrir el archivo.

## Alcance actual

Incluido:

- activación bajo demanda y creación lazy;
- un escritor y un pool de lectores por base;
- evicción LRU y timeout inactivo;
- WAL, claves foráneas, timeout de escritura y checkpoint en segundo plano;
- control de admisión por tenant y por proceso, con descarte explícito;
- endpoints de consulta, ejecución, batch, salud, estadísticas y métricas Prometheus;
- autenticación bearer, límites HTTP y apagado ordenado.

No incluido todavía:

- almacenamiento COLD/S3 o descarga automática;
- replicación o failover multi-instancia;
- migraciones administradas;
- credenciales/cuotas por tenant;
- backups programados;
- protocolo compatible con PostgreSQL/MySQL.

Estas exclusiones son deliberadas: el núcleo actual conserva el objetivo de consumo mínimo y un modelo
de consistencia sencillo de operar.
