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
3. Si ya existe, incrementa su lease y reutiliza el pool.
4. Si no existe, crea o abre `{tenant}.db` con WAL y los pragmas configurados.
5. Si se alcanzó `max_open_databases`, elimina primero la base inactiva menos reciente.
6. La operación SQL se ejecuta con parámetros posicionales.
7. Al terminar, el lease disminuye y actualiza la hora del último uso.

El mutex del administrador serializa únicamente la búsqueda/apertura de pools. Las consultas se
ejecutan fuera de ese mutex y pueden avanzar simultáneamente según `connections_per_database`.

## Dormir una base con seguridad

Una tarea periódica identifica entradas cuyo último uso supera `idle_timeout_seconds`. Solo retira una
entrada si su contador de leases es cero. Después ejecuta:

```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

y cierra el pool. Esto evita cortar consultas en curso y reduce el WAL antes de devolver la base al
estado dormido. Si el checkpoint no puede completarse, el cierre sigue siendo seguro: SQLite conserva
el WAL para recuperarlo al abrir de nuevo.

## Concurrencia y consistencia

SQLite permite múltiples lectores, pero solo un escritor por archivo. WAL mejora la convivencia entre
lecturas y escritura; no convierte SQLite en una base distribuida. `busy_timeout_ms` hace que una
escritura espere brevemente ante contención en lugar de fallar inmediatamente.

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
conexiones máximas ≈ max_open_databases × connections_per_database
```

Con 50 bases abiertas y 2 conexiones por base, el techo es aproximadamente 100 conexiones SQLite.
Puede haber miles de archivos dormidos sin crear miles de pools. El número apropiado depende de la
RAM, el patrón de consultas, el page cache y la latencia del volumen.

La API también limita:

- tamaño del body para evitar cargas descontroladas;
- duración de cada solicitud;
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
- pools limitados por base;
- evicción LRU y timeout inactivo;
- WAL, claves foráneas, timeout de escritura y checkpoint;
- endpoints de consulta, ejecución, batch, salud y estadísticas;
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
