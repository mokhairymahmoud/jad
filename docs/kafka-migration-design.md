# Kafka Migration Design — Phase 1 Research Findings

> **Date**: 2026-08-17  
> **EDC Version**: 0.19.0-SNAPSHOT (main branch)  
> **Status**: Phase 1 Complete — SPI Analysis

---

## Executive Summary

EDC uses NATS JetStream for **two separate concerns**, both implemented as pluggable extensions
in the main `eclipse-edc/Connector` repository:

1. **Event Publishing** — broadcasting domain events (contract negotiation lifecycle, transfer
   process lifecycle) to external consumers via CloudEvents over NATS JetStream
2. **Task Distribution** (Virtual Connector pattern) — distributing control-plane work
   (negotiations, transfers) across horizontally-scaled instances

**Critical finding**: EDC's core state machine does NOT depend on any message broker. It uses
pure **database-level leasing** (PostgreSQL `edc_lease` table with row-level locking) for
coordination. The NATS task distribution is an **optional overlay** used only in the "Virtual
Connector" deployment model.

This means replacing NATS with Kafka is **purely an extension swap** — no EDC core changes needed.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    EDC Control Plane                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────┐       ┌─────────────────────────────┐  │
│  │  StateMachineManager │       │      EventRouter            │  │
│  │  (poll loop)         │       │  (in-memory pub/sub)        │  │
│  │                      │       │                             │  │
│  │  ┌───────────────┐  │       │  Events: ContractNeg*,      │  │
│  │  │  Processor    │──┼───────┼──TransferProcess*, Asset*   │  │
│  │  │  (per state)  │  │       │         │                   │  │
│  │  └───────────────┘  │       └─────────┼───────────────────┘  │
│  │         │            │                 │                       │
│  └─────────┼────────────┘                 │                       │
│            │                              │                       │
│  ┌─────────▼────────────┐     ┌──────────▼──────────────────┐   │
│  │  TaskService          │     │  EventSubscriber             │   │
│  │  TaskObservable       │     │  (NatsEventPublisher or      │   │
│  │         │             │     │   KafkaEventPublisher)       │   │
│  │         │             │     └──────────┬──────────────────┘   │
│  │  ┌──────▼──────────┐ │                 │                       │
│  │  │ TaskListener     │ │                 │                       │
│  │  │ (NatsTaskPub or  │ │                 │                       │
│  │  │  KafkaTaskPub)   │ │                 │                       │
│  │  └──────┬───────────┘ │                 │                       │
│  └─────────┼─────────────┘                 │                       │
│            │                               │                       │
└────────────┼───────────────────────────────┼───────────────────────┘
             │                               │
             ▼                               ▼
┌────────────────────────┐       ┌────────────────────────────────┐
│   NATS / Kafka          │       │   NATS / Kafka                  │
│   Task Subjects/Topics  │       │   Event Stream/Topics           │
│                         │       │                                  │
│   • negotiation-tasks   │       │   • events.contract.negotiation.*│
│   • transfer-tasks      │       │   • events.transfer.process.*    │
└────────────────────────┘       └────────────────────────────────┘
             │                               │
             ▼                               ▼
┌────────────────────────┐       ┌────────────────────────────────┐
│  Task Subscriber        │       │  External Consumers             │
│  (other CP replicas)    │       │  (monitoring, audit, etc.)      │
└────────────────────────┘       └────────────────────────────────┘
```

---

## NATS Modules in EDC (what we're replacing)

All modules live in `eclipse-edc/Connector` (main branch):

### Event Publishing

| Module | Maven Artifact | Purpose |
|--------|---------------|---------|
| `extensions/common/events/events-nats` | `org.eclipse.edc:events-nats` | Publishes EDC domain events to NATS JetStream |

### Task Distribution (Virtual Connector)

| Module | Maven Artifact | Purpose |
|--------|---------------|---------|
| `extensions/control-plane/tasks/nats/publisher/negotiation-tasks-publisher-nats` | `org.eclipse.edc:negotiation-tasks-publisher-nats` | Publishes negotiation tasks |
| `extensions/control-plane/tasks/nats/publisher/transfer-tasks-publisher-nats` | `org.eclipse.edc:transfer-tasks-publisher-nats` | Publishes transfer tasks |
| `extensions/control-plane/tasks/nats/subscriber/negotiation-tasks-subscriber-nats` | `org.eclipse.edc:negotiation-tasks-subscriber-nats` | Consumes negotiation tasks |
| `extensions/control-plane/tasks/nats/subscriber/transfer-tasks-subscriber-nats` | `org.eclipse.edc:transfer-tasks-subscriber-nats` | Consumes transfer tasks |

### Authentication

| Module | Maven Artifact | Purpose |
|--------|---------------|---------|
| `extensions/common/nats/nats-auth-nkey` | `org.eclipse.edc:nats-auth-nkey` | NKey seed-based NATS auth |

### Shared Library

| Path | Contents |
|------|----------|
| `core/common/lib/core-lib/src/main/java/org/eclipse/edc/nats/` | `NatsFunctions.java` |
| `core/common/lib/core-lib/src/main/java/org/eclipse/edc/nats/tasks/publisher/` | `NatsTaskPublisher.java` |
| `core/common/lib/core-lib/src/main/java/org/eclipse/edc/nats/tasks/subscriber/` | NATS task subscriber |

### BOM

| Module | Includes |
|--------|----------|
| `dist/bom/controlplane-virtual-feature-nats-bom` | nats-auth-nkey + all 4 task pub/sub modules |

---

## SPI Interfaces to Implement

### 1. Event Publishing SPI

**Package**: `org.eclipse.edc.spi.event` (module `:spi:core-spi`)

```java
// The extension point — we register our Kafka publisher here
public interface EventRouter {
    void register(Class eventKind, EventSubscriber subscriber);
    void registerSync(Class eventKind, EventSubscriber subscriber);
    void publish(E event);
    void publish(EventEnvelope eventEnvelope);
}

// What we implement
public interface EventSubscriber {
    void on(EventEnvelope event);
}

// The event wrapper (Jackson-serializable, polymorphic)
public class EventEnvelope<E extends Event> {
    String id;       // UUID
    long at;         // timestamp
    E payload;       // the domain Event
}
```

**What we build**: A `KafkaEventPublisher implements EventSubscriber` that:
- Serializes `EventEnvelope` to JSON (matching the existing format)
- Wraps in CloudEvents envelope (matching `events-nats` behavior)
- Publishes to Kafka topic `events.<event-name>`

### 2. Task Distribution SPI

**Package**: `org.eclipse.edc.controlplane.tasks` (module `:spi:control-plane-spi`)

```java
// We register our publisher as a listener
@ExtensionPoint
public interface TaskObservable extends Observable<TaskListener> {}

// What we implement
public interface TaskListener {
    void created(Task task);
}

// The task entity (Jackson-serializable, polymorphic payload)
public class Task {
    String id;
    long at;
    int retryCount;
    String name;
    String group;
    TaskPayload payload;  // @JsonTypeInfo — polymorphic (ProcessTaskPayload, etc.)
}

// Used for subject/topic routing
public class ProcessTaskPayload implements TaskPayload {
    String getProcessType();  // "negotiations" or "transfers"
}
```

**What we build**: A `KafkaTaskPublisher implements TaskListener` that:
- On `created(Task task)`: serializes to JSON, publishes to Kafka topic based on `processType`
- Uses Kafka producer with `enable.idempotence=true`

And a `KafkaTaskSubscriber` that:
- Consumes from the matching Kafka topic using a consumer group
- Deserializes and executes tasks via the control plane executor

### 3. Observer SPI (used by TaskObservable)

```java
public interface Observable<T> {
    void registerListener(T listener);
    void unregisterListener(T listener);
    Collection<T> getListeners();
}
```

---

## Core State Machine (unchanged — no NATS dependency)

The control plane's internal coordination uses **purely PostgreSQL**:

```sql
CREATE TABLE IF NOT EXISTS edc_lease (
    leased_by      VARCHAR NOT NULL,      -- runtime ID
    leased_at      BIGINT,                -- posix timestamp
    lease_duration INTEGER NOT NULL,      -- milliseconds (default 60000)
    resource_id    VARCHAR NOT NULL,      -- entity ID
    resource_kind  VARCHAR NOT NULL,      -- type discriminator
    PRIMARY KEY(resource_id, resource_kind)
);
```

**This does NOT change.** The DB lease mechanism handles coordination. The NATS/Kafka task
distribution is an **optimization layer** on top — it distributes already-created tasks to
subscriber instances for execution.

---

## NATS → Kafka Concept Mapping

| NATS Concept | Kafka Equivalent | Notes |
|--------------|-----------------|-------|
| JetStream stream | Kafka topic | Persistent, replayed |
| Subject (`events.contract.negotiation.initiated`) | Topic name or topic + headers | Kafka doesn't have subject hierarchy; use topics or record headers |
| Work queue (competing consumers on same subject) | Consumer group | Exactly the same semantics |
| ACK/NAK | Offset commit / seek-back | Kafka auto-commit or manual commit |
| RetentionPolicy.Interest | Topic retention `delete` + consumer group tracking | Messages removed when all groups consumed |
| StorageType.Memory | Topic with `cleanup.policy=delete`, low `retention.ms` | Or just use defaults |
| NKey auth | SASL/SCRAM or mTLS | Kafka-native auth |
| CloudEvents wrapping | Same — CloudEvents envelope in JSON value | Keep the serialization format |
| Trace context in headers | Same — Kafka headers support arbitrary key-value | W3C traceparent propagation |

### Topic Design

| Current NATS Subject | Proposed Kafka Topic | Partitioning |
|---------------------|---------------------|--------------|
| `events.>` (stream: `edc-events`) | `edc-events` (single topic, event type in headers) | By `participantContextId` or round-robin |
| `{prefix}.negotiations.{name}` | `edc-negotiation-tasks` | By negotiation ID (ordering guarantee) |
| `{prefix}.transfers.{name}` | `edc-transfer-tasks` | By transfer process ID |

---

## Implementation Plan

### Module Structure (in this repo: `/Users/mahmoudm/dev/jad`)

```
extensions/
├── data-plane-certs/          (existing)
├── kafka-common/              (NEW - shared Kafka client config)
│   ├── build.gradle.kts
│   └── src/main/java/org/eclipse/edc/jad/kafka/
│       ├── KafkaCommonExtension.java
│       ├── KafkaClientFactory.java
│       └── KafkaConfig.java
├── kafka-events/              (NEW - replaces events-nats)
│   ├── build.gradle.kts
│   └── src/main/java/org/eclipse/edc/jad/kafka/events/
│       ├── KafkaEventPublishingExtension.java
│       └── KafkaEventPublisher.java
├── kafka-tasks-publisher/     (NEW - replaces negotiation/transfer-tasks-publisher-nats)
│   ├── build.gradle.kts
│   └── src/main/java/org/eclipse/edc/jad/kafka/tasks/publisher/
│       ├── KafkaNegotiationTasksPublisherExtension.java
│       ├── KafkaTransferTasksPublisherExtension.java
│       └── KafkaTaskPublisher.java
└── kafka-tasks-subscriber/    (NEW - replaces negotiation/transfer-tasks-subscriber-nats)
    ├── build.gradle.kts
    └── src/main/java/org/eclipse/edc/jad/kafka/tasks/subscriber/
        ├── KafkaNegotiationTasksSubscriberExtension.java
        ├── KafkaTransferTasksSubscriberExtension.java
        └── KafkaTaskSubscriber.java
```

### Configuration Properties

```properties
# Kafka Common
edc.kafka.bootstrap.servers=kafka.edc-v.svc.cluster.local:9092
edc.kafka.security.protocol=PLAINTEXT
edc.kafka.client.id=${edc.runtime.id}

# Event Publishing (replaces edc.events.nats.*)
edc.events.kafka.topic=edc-events
edc.events.kafka.topic.create=true

# Task Distribution (replaces edc.nats.cn.publisher.*)
edc.tasks.kafka.negotiation.topic=edc-negotiation-tasks
edc.tasks.kafka.transfer.topic=edc-transfer-tasks
edc.tasks.kafka.consumer.group=${edc.runtime.id}-tasks
edc.tasks.kafka.consumer.auto.offset.reset=earliest
```

### Dependencies to Add (`gradle/libs.versions.toml`)

```toml
[versions]
kafka = "3.9.0"
cloudevents = "4.0.1"

[libraries]
kafka-clients = { module = "org.apache.kafka:kafka-clients", version.ref = "kafka" }
cloudevents-core = { module = "io.cloudevents:cloudevents-core", version.ref = "cloudevents" }
cloudevents-json-jackson = { module = "io.cloudevents:cloudevents-json-jackson", version.ref = "cloudevents" }
```

---

## Deployment Changes

### Helm: Add Kafka, Remove NATS

**Option A (recommended for KinD local dev)**: Bitnami Kafka with KRaft mode (no ZooKeeper)

```yaml
# platform-override-values.yaml additions
kafka:
  enabled: true
  kraft:
    enabled: true
  listeners:
    client:
      protocol: PLAINTEXT
  controller:
    replicaCount: 1
  broker:
    replicaCount: 1
    resources:
      requests:
        memory: 256Mi
        cpu: 100m

nats:
  enabled: false  # disable NATS
```

### Control Plane Image

The control plane image is built from `eclipse-cfm/platform-images`. Two options:

1. **Fork platform-images** — swap NATS BOM for our Kafka extensions in the CP launcher
2. **Overlay via Helm** — mount Kafka extension JARs as init-container artifacts into the
   CP container's classpath

**Recommended**: Option 1 for clean builds. The CP launcher's `build.gradle.kts` changes from:
```kotlin
// REMOVE:
runtimeOnly(libs.edcv.bom.controlplane.nats)
runtimeOnly(libs.edc.events.nats)
runtimeOnly(libs.edc.nats.auth.nkey)

// ADD:
runtimeOnly(project(":extensions:kafka-common"))
runtimeOnly(project(":extensions:kafka-events"))
runtimeOnly(project(":extensions:kafka-tasks-publisher"))
runtimeOnly(project(":extensions:kafka-tasks-subscriber"))
```

---

## Risk Re-Assessment (Post-Research)

| Risk | Original | Revised | Reason |
|------|----------|---------|--------|
| NATS SPI not cleanly abstracted | High | **Low** ✅ | SPI is clean: `EventSubscriber` + `TaskListener`. Pure extension swap. |
| Kafka overhead for local dev | Medium | **Medium** | KRaft mode helps but Kafka still ~256MB vs NATS ~32MB |
| Exactly-once semantics gap | Medium | **Low** ✅ | EDC state machines are idempotent (DB lease ensures single processing). Kafka `enable.idempotence=true` on producer suffices. |
| Platform-images divergence | Medium | **Medium** | Still need to fork/overlay, but the change is minimal (dependency swap) |

**New risk identified**: The `NatsTaskPublisher` in `core-lib` uses NATS-specific subject formatting
(`{prefix}.{processType}.{name}`). Our Kafka equivalent needs to map this to topics, but the SPI
boundary (`TaskListener.created(Task)`) is clean — we just route differently.

---

## Next Steps (Phase 2)

1. Create the extension modules (`kafka-common`, `kafka-events`, `kafka-tasks-publisher`, `kafka-tasks-subscriber`)
2. Implement `KafkaEventPublisher` matching the CloudEvents + trace context format of `NatsEventPublisher`
3. Implement `KafkaTaskPublisher` and `KafkaTaskSubscriber` using Kafka consumer groups
4. Add Kafka client dependencies to the version catalog
5. Write unit tests for serialization compatibility
6. Update the dataplane Helm config to use Kafka instead of NATS
