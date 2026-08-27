# Benchmark 05: caché de sentencias preparadas

sqlx conserva por defecto hasta 100 sentencias preparadas por conexión SQLite. En un servidor con 50
bases calientes y dos conexiones por base, eso permite hasta 10.000 objetos de sentencia. El coste no
aparece en cargas que reutilizan unas pocas consultas parametrizadas, pero sí cuando los clientes
generan texto SQL diferente continuamente.

## Escenario controlado

- 50 bases activas.
- Una conexión lectora por base.
- Cuatro lectores de lazo cerrado por base y ningún escritor durante la medición.
- 100 textos SQL distintos por conexión.
- Todos los textos tenían el mismo plan y resultado; un comentario diferente alteraba únicamente la
  clave utilizada por el cache de sqlx.
- Duración de 20 segundos.

Eliminar escritores fue importante: en la carga mixta, la alineación de checkpoints y la cantidad de
updates variaban demasiado entre corridas para atribuir diferencias pequeñas al cache de sentencias.

## Resultados

| Capacidad por conexión | Pico | RAM privada | Throughput | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 272,61 MiB | 126,45 MiB | 2.980,8 ops/s | 63,41 ms | 116,20 ms | 163,37 ms |
| 32 | 276,46 MiB | 129,77 MiB | 2.996,1 ops/s | 63,42 ms | 117,60 ms | 154,98 ms |
| 100 | 287,40 MiB | 139,47 MiB | 2.991,7 ops/s | 63,55 ms | 114,85 ms | 157,32 ms |

Limitar el cache a 16 ahorró aproximadamente 13 MiB de RAM privada y 15 MiB de Working Set frente a
100, sin una pérdida medible de throughput. El ahorro observado equivale a unos 0,26 MiB privados por
conexión en este conjunto de sentencias.

## Decisión

El nuevo default es:

```toml
statement_cache_capacity = 16
```

Puede cambiarse con `TINKIVA_STATEMENT_CACHE_CAPACITY`. Aplicaciones con más de 16 consultas realmente
calientes por conexión pueden elevarlo; clientes que parametrizan correctamente suelen necesitar muy
pocas entradas.

La recomendación sigue siendo usar placeholders:

```sql
SELECT * FROM products WHERE id = ?;
```

en lugar de generar una cadena diferente por valor:

```sql
SELECT * FROM products WHERE id = 123;
SELECT * FROM products WHERE id = 456;
```
