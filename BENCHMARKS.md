# Pruebas de rendimiento y consumo de memoria

Este documento consolida las mediciones de TinkivaDatabase: cuánta RAM cuesta una base caliente, qué
capacidad tiene el servicio y qué se sabe con certeza frente a qué se está infiriendo.

> [!IMPORTANT]
> Estos resultados caracterizan una máquina y una carga concretas; no representan un límite universal.
> Antes de dimensionar producción, ejecuta `python tools/benchmark.py` en el servidor de destino con
> consultas y tamaños de respuesta similares a los reales.

## Cómo leer estos números

El arnés es de **lazo cerrado**: un número fijo de trabajadores manda la siguiente petición en cuanto
recibe la anterior. Eso tiene dos consecuencias que hay que tener presentes:

- El throughput es `usuarios ÷ latencia`, siempre. No mide la capacidad del servicio; mide cuántos
  clientes se pusieron a la vez.
- La cola no puede desaparecer. Si el servicio se vuelve más rápido, los mismos 250 clientes siguen
  manteniendo 250 peticiones en vuelo: la mediana baja y la varianza se va a los percentiles altos.
  Un cliente de lazo cerrado nunca puede mostrar el control de admisión haciendo su trabajo, porque
  nunca genera más carga de la que el servicio ya está absorbiendo.

Por eso cada escenario reporta **CPU del servidor y del cliente por separado**. La máquina tiene 16
CPU lógicas:

- Con 1 base, el cliente Python se queda clavado cerca de 1 core (el GIL) mientras el servidor usa
  entre 2,6 y 4,8. Ese escenario está limitado por el cliente y su throughput no dice nada del techo
  del servicio.
- Con 20 y 50 bases, el servidor usa 13–14 de los 16 cores. Ahí sí está saturado, y las cifras
  describen capacidad real.

La métrica que sí compara honestamente dos versiones bajo el mismo cliente es **CPU por operación**.

## Comparación con la versión anterior

Ambos binarios medidos con el mismo arnés, el mismo cliente, la misma máquina y 30 segundos por
escenario. `c7fa249` es la versión previa a la ronda de optimización; usaba un pool simétrico de dos
conexiones por base.

| Escenario | Versión | ops/s | p50 | p95 | p99 | CPU servidor | CPU ms/op | RAM pico |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 base, 5 usuarios | `c7fa249` | 3.458,4 | 1,39 ms | 2,32 ms | 2,80 ms | 3,46 cores | 1,000 | 23,56 MiB |
| 1 base, 5 usuarios | actual | **5.556,6** | 1,00 ms | 1,69 ms | 2,11 ms | 4,19 cores | 0,754 | 22,80 MiB |
| 20 bases, 100 usuarios | `c7fa249` | 2.595,1 | 36,08 ms | 66,44 ms | 81,74 ms | 13,15 cores | 5,068 | 77,45 MiB |
| 20 bases, 100 usuarios | actual | **4.695,9** | 10,20 ms | 58,82 ms | 96,16 ms | 13,09 cores | 2,787 | 117,01 MiB |
| 50 bases, 250 usuarios | `c7fa249` | 2.622,6 | 90,51 ms | 160,82 ms | 198,23 ms | 13,04 cores | 4,973 | 157,79 MiB |
| 50 bases, 250 usuarios | actual | **4.385,2** | 30,47 ms | 187,02 ms | 313,96 ms | 13,27 cores | 3,027 | 213,76 MiB |

Ninguna corrida produjo errores SQLite ni HTTP.

Lo que se puede afirmar: con 20 y 50 bases las dos versiones consumen prácticamente los mismos 13
cores, y con ese mismo presupuesto de CPU la versión actual hace **entre 67% y 81% más trabajo**. El
coste de CPU por operación cae de 5,07 a 2,79 ms con 20 bases y de 4,97 a 3,03 ms con 50.

Lo que empeoró, en dos frentes:

- **Memoria.** Con 50 bases el pico sube de 157,79 a 213,76 MiB.
- **Cola con 50 bases.** El p95 sube de 160,82 a 187,02 ms y el p99 de 198,23 a 313,96 ms, aunque la
  mediana mejora tres veces. Es el efecto de lazo cerrado descrito arriba: al triplicar el ritmo de
  servicio con el mismo número de clientes en vuelo, la varianza se concentra en la cola.

## Resultados por escenario

Versión actual, 30 segundos por escenario, cada base con 10.000 registros, un payload de 256 bytes
por registro, unos 2,72 MiB de datos, una tabla y un índice.

| Escenario | Bases | Lectores/base | Usuarios | ops/s | p50 | p95 | p99 | RAM final | RAM pico | CPU srv | CPU ms/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1db-read1` | 1 | 1 | 5 | 5.435,0 | 0,32 ms | 2,36 ms | 2,91 ms | 18,05 MiB | 18,64 MiB | 2,65 | 0,488 |
| `1db-read2` | 1 | 2 | 5 | 5.556,6 | 1,00 ms | 1,69 ms | 2,11 ms | 19,53 MiB | 22,80 MiB | 4,19 | 0,754 |
| `1db-read4` | 1 | 4 | 5 | 4.907,3 | 1,01 ms | 2,04 ms | 2,51 ms | 20,68 MiB | 24,89 MiB | 4,83 | 0,984 |
| `5db-read2` | 5 | 2 | 25 | 4.736,8 | 3,01 ms | 11,65 ms | 13,39 ms | 41,95 MiB | 43,77 MiB | 12,08 | 2,551 |
| `20db-read2` | 20 | 2 | 100 | 4.695,9 | 10,20 ms | 58,82 ms | 96,16 ms | 112,48 MiB | 117,01 MiB | 13,09 | 2,787 |
| `50db-read1` | 50 | 1 | 250 | 4.385,2 | 30,47 ms | 187,02 ms | 313,96 ms | 205,55 MiB | 213,76 MiB | 13,27 | 3,027 |
| `50db-read2` | 50 | 2 | 250 | 3.656,3 | 57,45 ms | 168,58 ms | 261,72 ms | 250,26 MiB | 270,31 MiB | 13,78 | 3,767 |
| `20db-writeheavy` | 20 | 2 | 100 | 6.258,1 | 14,50 ms | 31,47 ms | 41,55 ms | 91,76 MiB | 91,80 MiB | 2,44 | 0,389 |
| `20db-scan` | 20 | 2 | 100 | 5.012,7 | 15,22 ms | 72,52 ms | 104,02 ms | 144,50 MiB | 147,48 MiB | 9,02 | 1,800 |

El proceso vacío consumía aproximadamente **11,6 MiB**.

Dos escenarios merecen atención por lo que revelan del coste real:

- `20db-writeheavy` (cuatro escritores y un lector por base) alcanza el mayor throughput de todo el
  conjunto —6.258 ops/s— usando solo **2,44 cores**. Las escrituras son baratas: un `UPDATE` por
  clave primaria sin payload de vuelta. Lo caro es serializar respuestas.
- `20db-scan` usa 144,50 MiB de working set pero solo **32,94 MiB de memoria privada**. La diferencia
  son páginas mapeadas del archivo: compartidas, desalojables y respaldadas por disco. En Windows
  inflan el working set sin ser memoria comprometida, así que `private_mb` es la cifra honesta para
  dimensionar.

## Cuántas conexiones de lectura

Un escritor por base no es configurable: SQLite serializa las escrituras sobre el archivo sin importar
cuántas conexiones existan. Los lectores sí.

| Lectores | Contexto | ops/s | p95 | RAM pico | CPU ms/op |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 1 base | 5.435,0 | 2,36 ms | 18,64 MiB | 0,488 |
| 2 | 1 base | 5.556,6 | 1,69 ms | 22,80 MiB | 0,754 |
| 4 | 1 base | 4.907,3 | 2,04 ms | 24,89 MiB | 0,984 |
| 1 | 50 bases | 4.385,2 | 187,02 ms | 213,76 MiB | 3,027 |
| 2 | 50 bases | 3.656,3 | 168,58 ms | 270,31 MiB | 3,767 |

El coste de CPU por operación crece de forma monótona con cada lector añadido: 0,488, 0,754 y 0,984 ms
para 1, 2 y 4. El motivo es estructural: el driver de sqlx dedica **un hilo del sistema operativo a
cada conexión SQLite**, de modo que cada lector extra es un hilo más y un traspaso por canal más en
cada operación.

Con una sola base caliente y CPU de sobra, dos lectores compran mejor p95. Con 50 bases calientes en
una máquina ya saturada de CPU, un lector gana en throughput y en memoria a la vez. El default es 1
porque la densidad de tenants es el caso de uso del servicio:

```toml
reader_connections = 1
```

Esto no limita la aplicación a un usuario por base. Limita las lecturas SQLite simultáneas sobre un
archivo; el resto espera brevemente sin abrir conexiones nuevas.

## El techo del WAL

Los checkpoints los ejecuta una tarea de fondo, así que ninguna petición paga uno bajo carga normal.
`wal_size_limit_mb` es el techo al que una petición hace checkpoint como último recurso, y también el
tamaño al que se recorta el WAL después.

Barrido con 50 bases y un lector:

| Techo WAL | ops/s | p50 | p95 | p99 | RAM pico | RAM privada | WAL en disco |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 MiB | 4.454,1 | 34,47 ms | 170,02 ms | 275,70 ms | 216,61 MiB | 174,41 MiB | 396,64 MiB |
| 8 MiB | 3.518,4 | 66,54 ms | 165,01 ms | 222,50 ms | 212,75 MiB | 171,55 MiB | 291,46 MiB |
| 4 MiB | 3.804,0 | 56,77 ms | 164,76 ms | 235,75 ms | 212,61 MiB | 171,33 MiB | 200,00 MiB |
| 2 MiB | 3.860,4 | 53,83 ms | 174,18 ms | 277,62 ms | 192,27 MiB | 157,55 MiB | 100,00 MiB |

El WAL en disco escala exactamente con el techo: 50 bases × el límite configurado. La memoria, en
cambio, **apenas se mueve** entre 4 y 16 MiB. Este barrido se hizo para comprobar si el techo del WAL
explicaba el aumento de memoria de esta ronda; no lo explica.

El default se queda en 16 MiB porque es el único punto que da consistentemente ~4.400 ops/s y una
mediana cerca de 32 ms, frente a ~3.700 ops/s y 54–67 ms en los demás: menos checkpoints significa
menos amplificación de escritura. El precio es 8 MiB de WAL en disco por tenant activo y un p99 unos
40 ms peor. Un despliegue con muchos tenants y disco ajustado debería bajarlo.

## Memoria por base caliente

| Versión | Escala | RAM retenida por base | RAM privada total |
| --- | ---: | ---: | ---: |
| `c7fa249` | 20 bases | 2,27 MiB | 49,02 MiB |
| `c7fa249` | 50 bases | 1,99 MiB | 108,38 MiB |
| actual | 20 bases (2 lectores) | 5,04 MiB | 86,59 MiB |
| actual | 50 bases (1 lector) | 3,88 MiB | 175,33 MiB |
| actual | 50 bases (2 lectores) | 4,77 MiB | 194,87 MiB |

Comparando a igual número de conexiones —dos por base en ambos casos— la memoria retenida pasó de
1,99 a 3,88 MiB por base caliente. El barrido del WAL atribuye una parte pequeña: bajar el techo de
16 a 2 MiB solo recupera 16,86 MiB de memoria privada sobre 50 bases, unos 0,34 MiB por base. **El
resto no está atribuido**, y es lo primero que debería medir la siguiente ronda: aislar el efecto de
`mmap_size`, del caché de sentencias preparadas por conexión y del índice `-shm` de un WAL mayor.

Para dimensionar producción con el default actual:

```text
RAM ≈ proceso base (≈12 MiB)
    + bases calientes × 4 MiB retenidos
    + buffers de consultas y respuestas concurrentes
```

| DB calientes simultáneas | Medición observada | Presupuesto sugerido |
| ---: | ---: | ---: |
| 5 | 43,77 MiB | 128–256 MiB |
| 20 | 117,01 MiB | 384–512 MiB |
| 50 | 213,76 MiB | 768 MiB–1 GiB |

Estos cálculos deben basarse en bases **calientes**, no en el número total de archivos almacenados. Un
servidor puede conservar miles de bases dormidas mientras limita las activas mediante
`max_open_databases`. Una base dormida es solamente un archivo: no conserva pools, hilos ni caché.

## Límites de esta prueba

- El cliente y el servidor se ejecutaron en la misma máquina, compitiendo por las mismas 16 CPU.
- El generador es de lazo cerrado y no simula pausas humanas.
- Con una sola base el cliente Python es el factor limitante, no el servicio.
- Cada escenario duró 30 segundos, no varias horas.
- No se probaron BLOB grandes, joins complejos ni migraciones simultáneas.
- El control de admisión no llegó a activarse: ninguna corrida produjo un solo `429`. Comprobarlo
  requiere un generador de lazo abierto que mantenga una tasa de llegada fija.
- El rendimiento variará según CPU, sistema operativo y almacenamiento.

La siguiente fase debería usar consultas reales de la aplicación, un generador de lazo abierto, un
cliente en otra máquina y corridas de horas en lugar de minutos.

## Reproducir la prueba

```bash
cargo build --release
python tools/benchmark.py                       # los nueve escenarios, 60s cada uno
python tools/benchmark.py --only 50db-read1 --duration 30
```

Para comparar contra otra versión bajo el mismo cliente:

```bash
TINKIVA_BENCH_BINARY=/ruta/a/otro/tinkiva-database python tools/benchmark.py --only 20db-read2
```

El generador levanta instancias temporales del servidor, crea las bases, ejecuta los escenarios y
devuelve una línea JSON por escenario. Los directorios generados bajo `data/` están excluidos de Git y
pueden eliminarse después de la prueba.

## Historial de corridas

- [benchmarks/02.md](benchmarks/02.md) — corrida actual, con comparación contra `c7fa249`.
- [benchmarks/01.md](benchmarks/01.md) — primera corrida. Sus cifras quedaron obsoletas: se midieron
  con un arnés que abría una conexión TCP por operación y sobre la versión previa a esta ronda.
