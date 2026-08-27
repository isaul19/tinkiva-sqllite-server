# Benchmark 04: control de admisión con tasa fija

El generador anterior era de lazo cerrado: cada usuario esperaba su respuesta antes de enviar otra
solicitud. Cuando subía la latencia, el propio cliente reducía la oferta y nunca conseguía sobrecargar
el servidor; por eso nueve corridas anteriores produjeron cero respuestas `429`.

Esta ronda añade un scheduler de tasa fija. Las solicitudes se generan según el reloj aunque las
respuestas se retrasen. El arnés distingue:

- solicitudes ofrecidas;
- solicitudes realmente enviadas;
- respuestas exitosas;
- rechazos `429 overloaded`;
- descartes del cliente al llenar su cola local;
- otros errores HTTP o de transporte.

## Escenario

- 20 bases activas.
- Un escritor y cuatro lectores lógicos por base.
- Una conexión lectora y una escritora por base.
- Máximo 8 solicitudes admitidas por tenant y 256 en el proceso.
- 10.000 filas por base.
- Duración de 20 segundos.

## Escalón de carga

| Oferta | Enviadas | Exitosas/s | `429` | Descartes cliente | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2.000 req/s | 40.000 | 2.000,0 | 0 | 0 | 8,30 ms | 20,21 ms | 29,57 ms |
| 4.000 req/s | 68.559 | 2.439,8 | 19.763 | 11.441 | 69,33 ms | 161,95 ms | 193,23 ms |
| 8.000 req/s | 68.191 | 2.519,9 | 17.793 | 91.809 | 52,08 ms | 199,12 ms | 260,73 ms |

A 2.000 req/s el servidor absorbió toda la oferta sin rechazos. Por encima de su capacidad sostenida,
el control de admisión devolvió `429` y evitó convertir todo el exceso en una cola interna ilimitada.
Los descartes del cliente significan que el generador alcanzó su propio límite de 512 solicitudes
pendientes; se reportan para no confundirlos con capacidad del servidor.

## Barrido de espera de admisión

Se mantuvo una oferta de 4.000 req/s y se cambió únicamente cuánto puede esperar una solicitud por un
slot antes de recibir `429`.

| Espera | Exitosas/s | `429` | Descartes cliente | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20 ms | 2.439,8 | 19.763 | 11.441 | 69,33 ms | 161,95 ms | 193,23 ms |
| 50 ms | 2.499,3 | 18.368 | 11.646 | 98,75 ms | 192,64 ms | 223,25 ms |
| 250 ms | 2.557,3 | 8.620 | 20.233 | 108,44 ms | 399,92 ms | 439,56 ms |

Esperar 250 ms aumentó el throughput solo 2,3 % frente a 50 ms, pero casi duplicó p99 y mantuvo más
trabajo pendiente en el cliente. El nuevo default es:

```toml
admission_timeout_ms = 50
```

Un servicio que prefiera fallar rápido y reintentar en otro shard puede usar 20 ms. Una carga batch que
prefiera throughput sobre latencia puede elevar el valor explícitamente.

## Reproducibilidad

Los escenarios `20db-openloop-*` viven en `tools/benchmark.py`. Por ejemplo:

```bash
python tools/benchmark.py --duration 20 --only 20db-openloop-4000-wait50
```

El arnés limita su backlog para no consumir memoria sin control. Para medir ofertas aún mayores será
necesario un cliente asíncrono o distribuido que pueda sostener la tasa sin descartes locales.
