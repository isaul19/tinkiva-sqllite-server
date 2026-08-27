# Benchmark 03: caché de páginas por rol

Esta ronda aisló el aumento de memoria privada observado después de separar el escritor y los lectores.
La causa dominante no era el WAL, `mmap_size` ni el archivo `-shm`: era aplicar un caché de páginas de
2.000 KiB a cada conexión.

## Carga principal

- 50 bases activas y 10.000 filas por base.
- Un escritor y cuatro lectores por base.
- Un writer pool de una conexión y un reader pool de una conexión.
- Duración de 30 segundos.
- Binario `release`, Windows, cliente de lazo cerrado.

| Caché escritor | Caché lector | Pico | RAM privada | Throughput | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2.000 KiB | 2.000 KiB | 212,0 MiB | 173,6 MiB | 4.365,5 ops/s | 34,35 ms | 180,48 ms | 310,09 ms |
| 1.024 KiB | 512 KiB | 124,0 MiB | 46,5 MiB | 4.153,0 ops/s | 46,98 ms | 165,86 ms | 247,16 ms |
| 1.536 KiB | 256 KiB | 136,0 MiB | 46,0 MiB | 4.200,8 ops/s | 42,82 ms | 179,88 ms | 304,49 ms |

El presupuesto 1.024/512 redujo el pico un **42 %** y la RAM privada un **73 %**, conservando
aproximadamente el 95 % del throughput. Dar 512 KiB a ambas conexiones redujo aún más memoria, pero
los resultados repetidos mostraron una caída de throughput cercana al 18 %. El conjunto de trabajo del
escritor necesita más caché que el lector indexado en esta carga.

## Cargas adversas

Se comparó 1.024/512 contra 2.000/2.000 con 20 bases y dos lectores por pool.

| Escenario | Caché | Pico | RAM privada | Throughput | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Escritura intensiva | 2.000/2.000 | 94,3 MiB | 79,2 MiB | 5.149,6 ops/s | 38,42 ms | 50,59 ms |
| Escritura intensiva | 1.024/512 | 72,8 MiB | 54,8 MiB | 5.104,3 ops/s | 38,94 ms | 51,24 ms |
| Escaneo completo | 2.000/2.000 | 148,2 MiB | 34,0 MiB | 4.870,1 ops/s | 73,00 ms | 102,75 ms |
| Escaneo completo | 1.024/512 | 147,9 MiB | 31,9 MiB | 4.655,3 ops/s | 65,31 ms | 85,04 ms |

En escritura intensiva, el nuevo presupuesto ahorró 23 % del pico con una pérdida de throughput menor
al 1 %. En el escaneo, el Working Set estuvo dominado por páginas mapeadas y casi no cambió; la RAM
privada bajó y el throughput fue 4,4 % menor.

## `mmap_size`

Desactivar `mmap` no resolvió la memoria. En la carga de 50 bases redujo el Working Set de 212 a unos
199 MiB, pero aumentó la RAM privada de 173,6 a 194,9 MiB y redujo ligeramente el throughput. El mapeo
estaba reemplazando memoria privada por páginas de archivo desalojables, por lo que se conserva el
default de 64 MiB.

## Decisión

Los nuevos defaults son:

```toml
writer_cache_size_kb = 1024
reader_cache_size_kb = 512
```

`cache_size_kb` sigue aceptándose como override común para configuraciones existentes. Las variables
`TINKIVA_WRITER_CACHE_SIZE_KB` y `TINKIVA_READER_CACHE_SIZE_KB` permiten ajustar cada rol por separado.

Las cifras tienen variación entre corridas por el cliente de lazo cerrado y la alineación de los
checkpoints. Deben interpretarse como comparación de configuraciones, no como capacidad garantizada.
