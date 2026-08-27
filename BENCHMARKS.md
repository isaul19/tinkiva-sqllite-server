# Pruebas de rendimiento y consumo de memoria

Este documento registra las primeras pruebas de carga de TinkivaDatabase. Su objetivo es estimar el
consumo de RAM por base activa y observar el comportamiento de SQLite con lectores y escritores
concurrentes.

> [!IMPORTANT]
> Estos resultados caracterizan una máquina y una carga concretas; no representan un límite universal.
> Antes de dimensionar producción, ejecuta `python tools/benchmark.py` en el servidor de destino con
> consultas y tamaños de respuesta similares a los reales.

## Resumen ejecutivo

Para el patrón probado —cinco usuarios por base activa, uno escribiendo y cuatro leyendo— se puede usar
esta estimación inicial:

| Componente | Consumo estimado |
| --- | ---: |
| Proceso base | 10–20 MiB |
| RAM retenida por DB activa | 2–3 MiB |
| Pico corto por DB activa | 3–5 MiB |
| Presupuesto conservador por DB caliente | 6–10 MiB |

Con 50 bases activas simultáneamente se midió un pico de aproximadamente **151 MiB**. Para absorber
consultas más pesadas, ráfagas, buffers y diferencias de plataforma, la recomendación inicial es:

| Entorno | RAM sugerida |
| --- | ---: |
| Pruebas o carga moderada | 512 MiB |
| Producción con margen cómodo | 1 GiB |

Las bases dormidas son solamente archivos: no conservan pool ni añaden este coste por base activa.

## Metodología

Las mediciones se realizaron el 26 de agosto de 2026 en Windows, usando el binario compilado con el
perfil `release`.

Cada base contenía:

- 10.000 registros;
- un payload de texto de 256 bytes por registro;
- aproximadamente 2,72 MiB de datos;
- una tabla y un índice;
- WAL y la configuración normal del servidor.

La preparación de cada base ejecutó:

1. `CREATE TABLE`;
2. `CREATE INDEX`;
3. una inserción masiva dentro de una transacción.

Después se mantuvieron cinco usuarios sin pausas durante ocho segundos por base:

- un escritor ejecutando continuamente un `UPDATE` parametrizado por clave primaria;
- cuatro lectores ejecutando `SELECT` indexados;
- cada lectura devolvía hasta 100 registros, incluyendo el payload.

Salvo en la comparación explícita de pools, cada base tenía un máximo de dos conexiones. Los cinco
usuarios compartían esas conexiones y esperaban en el pool cuando ambas estaban ocupadas.

## Resultados multi-tenant

| DB activas | Usuarios | Pool/DB | RAM máxima | RAM final | Rendimiento | Latencia p50 | Latencia p95 | Errores |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5 | 2 | 19,94 MiB | 18,97 MiB | 1.001,6 ops/s | 2,58 ms | 20,68 ms | 0 |
| 5 | 25 | 2 | 29,84 MiB | 29,75 MiB | 2.658,6 ops/s | 9,19 ms | 14,18 ms | 0 |
| 20 | 100 | 2 | 72,54 MiB | 51,45 MiB | 2.511,0 ops/s | 38,86 ms | 61,03 ms | 0 |
| 50 | 250 | 2 | 150,57 MiB | 103,05 MiB | 2.498,0 ops/s | 97,29 ms | 156,28 ms | 0 |

El proceso vacío consumía aproximadamente **10,4 MiB**. La prueba no produjo errores SQLite ni errores
HTTP.

El rendimiento total se estabilizó cerca de 2.500 operaciones por segundo a partir de 20 bases. Al
seguir aumentando usuarios sobre la misma máquina, el throughput dejó de crecer y la espera apareció
como latencia: el p95 pasó de 61,03 ms con 20 bases a 156,28 ms con 50.

## Consumo de una DB con cinco usuarios

Cuando solamente existía una base activa:

```text
Proceso vacío:             10,4 MiB
Proceso durante la carga:  19,9 MiB pico
Incremento observado:       9,5 MiB
```

Los 9,5 MiB no son el coste repetible de cada base. Incluyen el primer uso del allocator, runtime HTTP,
buffers y conexiones iniciales. Esos recursos se comparten y su coste se amortiza al abrir más tenants.

| Escala | RAM retenida adicional por DB | Pico adicional por DB |
| ---: | ---: | ---: |
| 5 DB | 3,87 MiB | 3,89 MiB |
| 20 DB | 2,05 MiB | 3,11 MiB |
| 50 DB | 1,85 MiB | 2,80 MiB |

En esta prueba, el coste incremental estabilizado fue de aproximadamente **2 MiB retenidos por base**
y **3 MiB de pico por base**. Para producción conviene presupuestar 6–10 MiB por DB caliente, dejando
un margen mínimo de 2× para consultas y cargas distintas.

## Pool de dos frente a cinco conexiones

El escenario de una base y cinco usuarios se repitió permitiendo cinco conexiones, de modo que cada
usuario pudiera ocupar potencialmente una conexión.

| Pool por DB | RAM máxima | RAM final | Rendimiento | Latencia p50 | Latencia p95 | Errores |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 conexiones | 19,94 MiB | 18,97 MiB | 1.001,6 ops/s | 2,58 ms | 20,68 ms | 0 |
| 5 conexiones | 23,65 MiB | 22,17 MiB | 749,1 ops/s | 3,63 ms | 24,87 ms | 0 |

El pool de cinco consumió unos 3,7 MiB adicionales y rindió peor. SQLite permite varios lectores, pero
sigue serializando las escrituras; aumentar conexiones puede añadir contención sin aumentar capacidad.

Para el patrón de un escritor y cuatro lectores, el punto de partida recomendado continúa siendo:

```toml
connections_per_database = 2
```

Esto no limita la aplicación a dos usuarios. Limita las operaciones SQLite simultáneas por archivo;
los demás usuarios esperan brevemente en el pool sin abrir conexiones adicionales.

## Base diez veces mayor

Para comprobar si el tamaño total del archivo se convertía directamente en RAM, se repitió el escenario
durante 15 segundos con:

- 100.000 registros;
- aproximadamente 27,1 MiB en disco;
- un escritor y cuatro lectores;
- un pool de dos conexiones.

| RAM máxima | RAM final | Rendimiento | Latencia p50 | Latencia p95 | Errores |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 19,99 MiB | 18,82 MiB | 923,9 ops/s | 2,83 ms | 22,39 ms | 0 |

La memoria casi no cambió frente a la base de 2,72 MiB. SQLite no carga el archivo completo: lee y
retiene las páginas necesarias para el conjunto de trabajo. Este resultado puede cambiar con escaneos
completos, índices ausentes, BLOB grandes o respuestas con muchas filas.

## Qué significan estos números

Una aproximación útil para el escenario medido es:

```text
RAM ≈ proceso base
    + bases actualmente calientes × 2–3 MiB retenidos
    + buffers de consultas y respuestas concurrentes
```

Para dimensionar con prudencia:

```text
RAM de producción ≈ proceso base
                  + bases calientes × 6–10 MiB
```

Ejemplos conservadores:

| DB calientes simultáneas | Medición observada | Presupuesto sugerido |
| ---: | ---: | ---: |
| 5 | 29,84 MiB | 128–256 MiB |
| 20 | 72,54 MiB | 256–512 MiB |
| 50 | 150,57 MiB | 512 MiB–1 GiB |

Estos cálculos deben basarse en bases **calientes**, no en el número total de archivos almacenados. Un
servidor puede conservar miles de bases dormidas mientras limita las activas mediante
`max_open_databases`.

## Limitaciones de la prueba

- Las consultas estaban indexadas y devolvían como máximo 100 filas.
- Los workers no simulaban pausas humanas; generaban presión continua.
- Cada escenario duró entre 8 y 15 segundos, no varias horas.
- No se probaron BLOB grandes, joins complejos, escaneos completos ni migraciones simultáneas.
- El cliente y el servidor se ejecutaron en la misma máquina.
- El rendimiento variará según CPU, sistema operativo y almacenamiento.

La siguiente fase de benchmarking debería utilizar consultas reales de la aplicación, pruebas de mayor
duración y monitoreo de CPU, IOPS, tamaño del WAL y percentiles de latencia.

## Reproducir la prueba

Primero compila el binario optimizado:

```bash
cargo build --release
```

Después ejecuta:

```bash
python tools/benchmark.py
```

El generador levanta instancias temporales del servidor, crea las bases, ejecuta los escenarios y
devuelve una línea JSON por escenario. Los directorios generados bajo `data/` están excluidos de Git y
pueden eliminarse después de la prueba.
