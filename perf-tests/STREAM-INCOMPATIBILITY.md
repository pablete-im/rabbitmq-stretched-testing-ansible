# Incompatibilidad de Streams entre OMQ y perf-test

## Problema

Los streams creados por OMQ y perf-test **NO son compatibles entre sí** debido a diferencias en los argumentos de declaración de la cola.

### Error Típico

```
PRECONDITION_FAILED - inequivalent arg 'x-max-length-bytes' for queue 'stream-baseline' 
in vhost '/': received the value '20000000000' of type 'long' but current is none
```

## Causa Raíz

### OMQ (AMQP 1.0)
- Crea streams **SIN** el argumento `x-max-length-bytes`
- Usa argumentos mínimos para máxima compatibilidad

### perf-test (AMQP 0.9.1)
- Crea streams **CON** `x-max-length-bytes=20000000000` (20GB por defecto)
- Usa configuración optimizada para testing

### Por qué Falla

RabbitMQ rechaza operaciones sobre colas existentes cuando los argumentos declarados son diferentes, devolviendo `PRECONDITION_FAILED`.

## Soluciones

### ✅ Solución 1: Borrar el Stream Antes de Cambiar de Herramienta (RECOMENDADO)

```bash
# Opción A: Con rabbitmqctl (requiere acceso SSH al servidor)
rabbitmqctl delete_queue stream-baseline

# Opción B: Con rabbitmqadmin (requiere plugin de management)
rabbitmqadmin delete queue name=stream-baseline

# Opción C: Con curl (API de management)
curl -u admin:password -X DELETE \
  http://localhost:15672/api/queues/%2F/stream-baseline
```

### ✅ Solución 2: Usar Streams Diferentes para Cada Herramienta

```yaml
# Para OMQ
name: test-omq
queue_type: stream
queue: stream-omq  # Nombre único

# Para perf-test
name: test-perf
queue_type: stream
queue: stream-perf  # Nombre diferente
```

### ✅ Solución 3: Usar Solo una Herramienta para Streams

**Opción A: Solo OMQ (AMQP 1.0)**
```bash
./run-test-omq.sh stream-baseline --hosts ... --protocol amqp-amqp
```

**Opción B: Solo perf-test (AMQP 0.9.1)**
```bash
# Escenario con type: amqp
./run-test.sh stream-baseline --hosts ...
```

## Diferencias Técnicas

### OMQ Stream Declaration

```
Queue: stream-baseline
Type: stream
Arguments: (ninguno o mínimos)
```

### perf-test Stream Declaration

```
Queue: stream-baseline
Type: stream
Arguments:
  x-max-length-bytes: 20000000000
  (otros argumentos de optimización)
```

## Escenarios de Ejemplo

### ❌ Flujo que FALLA

```bash
# 1. Crear stream con OMQ
./run-test-omq.sh stream-baseline --hosts localhost --password test123

# 2. Intentar usar el mismo stream con perf-test
./run-test.sh stream-baseline --hosts localhost --password test123
# ERROR: PRECONDITION_FAILED
```

### ✅ Flujo que FUNCIONA (Borrar entre ejecuciones)

```bash
# 1. Crear stream con OMQ
./run-test-omq.sh stream-baseline --hosts localhost --password test123

# 2. BORRAR el stream
rabbitmqctl delete_queue stream-baseline

# 3. Crear stream con perf-test (argumentos diferentes)
./run-test.sh stream-baseline --hosts localhost --password test123
# SUCCESS
```

### ✅ Flujo que FUNCIONA (Nombres diferentes)

```yaml
# scenarios/stream-omq.yml
name: stream-omq
type: amqp
queue_type: stream
queue: stream-omq  # Nombre único
```

```yaml
# scenarios/stream-perf.yml
name: stream-perf
type: amqp
queue_type: stream
queue: stream-perf  # Nombre diferente
```

```bash
./run-test-omq.sh stream-omq --hosts localhost --password test123
./run-test.sh stream-perf --hosts localhost --password test123
# Ambos funcionan porque usan streams diferentes
```

## Recomendaciones

### Para Testing de Compatibilidad
Si quieres probar la **compatibilidad entre protocolos** (ej: MQTT publisher → AMQP consumer):
- **Usa OMQ** exclusivamente para ambos lados
- OMQ soporta 16 combinaciones de protocolos

### Para Testing de Rendimiento AMQP 0.9.1
Si solo necesitas testing de rendimiento con AMQP 0.9.1:
- **Usa perf-test** exclusivamente
- Más opciones y mejor integración con AMQP 0.9.1

### Para Testing de Rendimiento Multi-Protocolo
Si necesitas testing con AMQP 1.0, MQTT, STOMP, etc.:
- **Usa OMQ** exclusivamente
- Soporte completo para múltiples protocolos

## Comandos Útiles

### Verificar argumentos de un stream existente

```bash
# Con rabbitmqctl
rabbitmqctl list_queues name type arguments | grep stream-baseline

# Con rabbitmqadmin
rabbitmqadmin show queue name=stream-baseline

# Con API
curl -u admin:password http://localhost:15672/api/queues/%2F/stream-baseline
```

### Borrar todos los streams

```bash
# Borrar todos los streams del vhost /
rabbitmqctl list_queues name type | grep stream | awk '{print $1}' | \
  xargs -I {} rabbitmqctl delete_queue {}
```

## Conclusión

La incompatibilidad es **por diseño**: cada herramienta optimiza los streams para su caso de uso específico. La solución más simple es **borrar el stream** antes de cambiar de herramienta, o usar **nombres diferentes** para cada herramienta.
