# Real-Time Analytics System

A distributed, fault-tolerant, real-time analytics platform powered by a **custom-built distributed execution engine** written in Golang.

Inspired by the architecture of large-scale data processing systems such as Apache Spark and Apache Flink, this project explores the fundamental challenges of distributed computing by implementing its own task scheduler, worker discovery, distributed execution, fault recovery, state management, and result aggregation.

The platform processes both historical and live transaction data across heterogeneous computing environments, including **AWS EC2, Google Cloud Compute Engine, and bare-metal machines**.

Rather than relying on existing distributed compute frameworks, the system implements the core mechanisms of a distributed execution engine from scratch.

The engine coordinates multiple machines to process large-scale datasets concurrently, recover from worker failures, and serve continuously updated analytical metrics through ClickHouse and a Kotlin Query API to a React dashboard.

## Project Objectives

The primary goal is to understand and implement how distributed computing frameworks coordinate workloads across multiple machines.

The custom distributed execution engine must be capable of:

1. **Worker Discovery:** Discover available machines across different computing environments.
2. **Task Partitioning:** Divide large datasets or workloads into independent tasks.
3. **Distributed Scheduling:** Determine which available worker should execute each task.
4. **Task Dispatch:** Send tasks to remote machines for execution.
5. **Worker Monitoring:** Track task progress and machine health through heartbeats.
6. **Fault Tolerance:** Detect failed workers and reassign their unfinished tasks.
7. **Result Aggregation:** Collect and combine distributed task outputs into a final result.

Additional objectives include:

- Support both historical batch processing and continuous event processing.
- Schedule workloads based on available CPU, memory, and worker capacity.
- Execute workloads across multiple cloud providers and physical machines.
- Maintain distributed job and task states.
- Implement retry policies and recovery mechanisms.
- Support checkpointing and recovery for stateful computations.
- Expose real-time execution metrics and worker health.
- Integrate distributed compute results into an analytical dashboard.

## System Architecture

```text
                         ┌──────────────────────┐
                         │    React Dashboard   │
                         │      TypeScript      │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    Kotlin Query API  │
                         │   Analytics & Jobs   │
                         └──────────┬───────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  │                                   │
                  ▼                                   ▼
       ┌──────────────────────┐          ┌────────────────────┐
       │  Distributed Engine  │          │ ClickHouse (OLAP)  │
       │     Coordinator      │          │ Analytical Storage │
       └──────────┬───────────┘          └──────────▲─────────┘
                  │                                  │
       ┌──────────┴───────────────┐                  │
       │                          │                  │
       ▼                          ▼                  │
┌────────────────┐       ┌────────────────┐          │
│ Job Scheduler  │       │ Worker Manager │          │
│ Task Placement │       │ Heartbeats     │          │
│ Retries        │       │ Health Checks  │          │
└────────┬───────┘       └────────┬───────┘          │
         │                        │                  │
         └────────────┬───────────┘                  │
                      │                              │
                      ▼                              │
         ┌─────────────────────────┐                 │
         │   Distributed Task      │                 │
         │   Dispatcher            │                 │
         └────────────┬────────────┘                 │
                      │                              │
          ┌───────────┼────────────┐                 │
          │           │            │                 │
          ▼           ▼            ▼                 │
     ┌─────────┐ ┌─────────┐ ┌─────────┐             │
     │ Worker A│ │ Worker B│ │ Worker C│             │
     │   AWS   │ │   GCP   │ │Bare Metal│             │
     └────┬────┘ └────┬────┘ └────┬────┘             │
          │           │           │                  │
          └───────────┼───────────┘                  │
                      │                              │
                      ▼                              │
         ┌─────────────────────────┐                 │
         │ Distributed Results &   │─────────────────┘
         │ State Aggregation       │
         └─────────────────────────┘
```

### Data Sources

The execution engine supports two primary input paths:

**Historical Data (Batch Processing)**

```text
S3 / MinIO
    |
    v
Historical Dataset
    |
    v
Task Partitioner
    |
    v
Distributed Worker Pool
    |
    v
Partial Results
    |
    v
Result Aggregator
    |
    v
ClickHouse
```

**Live Data (Stream Processing)**

```text
Event Generator
    |
    v
Apache Kafka
    |
    v
Stream Partition Assignment
    |
    v
Distributed Worker Pool
    |
    v
Stateful Event Processing
    |
    v
ClickHouse
    |
    v
React Dashboard
```

The custom execution engine coordinates both processing paths, while Kafka provides durable event ingestion and ClickHouse provides analytical storage.

## Core Architecture

The platform consists of seven primary components.

### 1. Distributed Coordinator

The coordinator is responsible for managing the overall execution of distributed workloads.

Responsibilities:

- Accept incoming job submissions.
- Generate execution plans.
- Partition workloads into tasks.
- Maintain the global task queue.
- Assign tasks to available worker machines.
- Track the execution status of jobs.
- Detect task failures and initiate retries.
- Coordinate the aggregation of completed task outputs.

The coordinator maintains a centralized view of the distributed cluster.

### 2. Worker Discovery & Cluster Membership

Workers can run across multiple environments:

- AWS EC2 instances
- Google Cloud Compute Engine instances
- Local physical machines
- Bare-metal Linux servers
- Docker containers for local development

Each worker registers with the coordinator when it starts.

Example worker registration:

```json
{
  "workerId": "worker-aws-01",
  "host": "10.0.1.15",
  "port": 9090,
  "provider": "AWS",
  "cpuCores": 8,
  "memoryMB": 16384,
  "availableSlots": 8,
  "status": "HEALTHY"
}
```

Worker machines periodically send heartbeat messages to the coordinator.

Heartbeats communicate:

- Worker availability
- CPU and memory utilization
- Available execution slots
- Running task identifiers
- Worker health status

If a worker fails to send heartbeats within a configurable timeout, the coordinator marks the worker as unavailable and schedules recovery of its unfinished tasks.

### 3. Task Partitioning

Large workloads are divided into smaller independent tasks that can execute concurrently.

For example, suppose a historical dataset contains 100 million transactions.

Instead of assigning the entire dataset to one machine, the coordinator partitions it into 100 tasks, each responsible for processing approximately 1 million records.

```text
100 Million Transaction Records
               |
               v
        Task Partitioner
               |
     ┌─────────┼──────────┐
     │         │          │
     ▼         ▼          ▼
  Task 1    Task 2      Task N
     │         │          │
     ▼         ▼          ▼
 Worker A   Worker B    Worker C
```

Tasks contain references to the data they must process, rather than transferring entire datasets through the coordinator.

Example:

```json
{
  "taskId": "task-001",
  "jobId": "job-123",
  "operation": "AGGREGATE_REVENUE",
  "input": {
    "bucket": "transaction-history",
    "objectKey": "transactions/2026/01.parquet"
  },
  "partition": {
    "startRow": 0,
    "endRow": 999999
  },
  "status": "PENDING"
}
```

Partitioning strategies may include:

- File-based partitioning
- Fixed-size data partitions
- Hash-based partitioning
- Key-based partitioning
- Kafka topic-partition assignment

Stateful workloads must preserve key ownership and processing order where required.

### 4. Distributed Task Scheduler

The scheduler determines which worker should execute each task.

The initial implementation uses a simple available-slot scheduling policy.

The scheduler considers:

- Worker health
- Available execution slots
- CPU and memory capacity
- Currently executing tasks
- Task resource requirements

Future scheduling strategies may include:

**Round Robin**

Distribute tasks evenly across available workers.

**Least Loaded**

Prefer workers with the most available resources.

**Data Locality Aware**

Prefer workers located near the data they need to process.

**Resource Aware**

Assign CPU-heavy or memory-heavy tasks to workers with sufficient resources.

**Cloud Cost Aware**

Consider compute and network transfer costs when assigning workloads across cloud providers.

The project will compare scheduling strategies using job completion time, worker utilization, and scheduling overhead.

### 5. Distributed Task Execution

Each worker executes tasks assigned by the coordinator.

Worker responsibilities:

1. Register with the coordinator.
2. Advertise available resources.
3. Receive task assignments.
4. Retrieve input data from object storage or Kafka.
5. Execute the assigned computation.
6. Persist task outputs or checkpoints.
7. Report execution status.
8. Return result metadata to the coordinator.

Communication between the coordinator and workers will initially use HTTP or gRPC.

Example task lifecycle:

```text
PENDING
   |
   v
SCHEDULED
   |
   v
RUNNING
   |
   ├───────────────┐
   │               │
   v               v
COMPLETED        FAILED
                   |
                   v
                 RETRY
                   |
                   v
                RUNNING
```

The engine tracks task attempts separately to prevent stale task executions from overwriting results produced by newer attempts.

### 6. Fault Tolerance & Recovery

Distributed systems must continue operating when individual machines fail.

The engine implements failure detection and recovery through worker heartbeats, persistent task metadata, and retry mechanisms.

**Worker Failure Scenario**

```text
Coordinator
    |
    v
Assign Task 42
    |
    v
Worker B
    |
    v
Worker B Crashes
    |
    v
Heartbeat Timeout
    |
    v
Mark Worker Unhealthy
    |
    v
Requeue Task 42
    |
    v
Assign Task 42 to Worker C
    |
    v
Worker C Completes Task
```

The coordinator must account for the possibility that a failed or unresponsive worker may still be executing an old task attempt.

To prevent duplicate results:

- Task assignments use unique attempt identifiers.
- Task outputs are written using deterministic or attempt-scoped identifiers.
- Completed results are committed using an idempotent protocol.
- Stale attempts are rejected when their ownership is no longer valid.

The initial engine targets **at-least-once execution with idempotent output commits**, rather than claiming universal exactly-once execution.

### 7. Distributed Result Aggregation

Once workers complete their tasks, the coordinator combines their outputs into a final result.

For example, calculating total revenue across a distributed dataset:

```text
Worker A: $120,000
Worker B: $95,000
Worker C: $135,000
          |
          v
    Result Aggregator
          |
          v
    Total: $350,000
```

The aggregation system supports operations such as:

- SUM
- COUNT
- MIN / MAX
- AVG using partial sums and counts
- Grouped aggregation
- Revenue by merchant
- Monthly recurring revenue calculations

For operations that are not trivially mergeable, the engine may require additional partitioning or shuffle stages.

Large intermediate results will be stored in shared storage rather than transferred entirely through the coordinator.

Final analytical outputs are written into ClickHouse for low-latency querying.

## Batch Processing Engine

The batch engine processes historical datasets stored in S3-compatible object storage.

Example use cases:

- Recalculate historical revenue.
- Rebuild monthly recurring revenue metrics.
- Process large transaction archives.
- Backfill missing analytical data.
- Recompute business metrics after definition changes.

### Batch Execution Flow

1. A user submits a batch job.
2. The coordinator examines the input dataset.
3. The task partitioner generates independent work units.
4. The scheduler assigns tasks to workers.
5. Workers retrieve and process their assigned partitions.
6. Workers persist partial results.
7. The coordinator validates successful task completion.
8. Aggregation tasks combine the partial results.
9. The final output is committed to ClickHouse.

The initial implementation will support single-stage parallel jobs followed by an aggregation stage.

Future versions may represent jobs as directed acyclic graphs (DAGs), allowing dependent computation stages and shuffle operations.

## Stream Processing Engine

The stream processing layer consumes live transaction events from Apache Kafka.

Unlike batch jobs, stream processing tasks are long-running and continuously process incoming events.

Example event:

```json
{
  "eventId": "evt-123",
  "merchantId": "merchant-001",
  "eventType": "PAYMENT_SUCCEEDED",
  "amount": 2500,
  "currency": "USD",
  "timestamp": "2026-11-15T12:00:00Z"
}
```

### Streaming Responsibilities

- Assign Kafka partitions to workers.
- Consume events continuously.
- Maintain per-key analytical state.
- Update revenue and subscription metrics.
- Persist checkpoints.
- Recover processing after worker failures.
- Publish updated analytical results.

### Stateful Processing

Some analytical metrics require historical context.

For example, monthly recurring revenue depends on active subscriptions, cancellations, upgrades, and downgrades.

Each worker maintains state for the keys it owns.

Example:

```text
Merchant A
    |
    v
Subscription Events
    |
    v
Stateful Worker
    |
    v
Current Revenue State
    |
    v
ClickHouse
```

The initial version may implement simple local state with periodic durable snapshots.

Later versions may integrate RocksDB for larger stateful workloads.

### Checkpointing & Recovery

Workers periodically persist application state and associated Kafka offsets.

If a worker crashes:

1. The coordinator detects the failure.
2. The affected Kafka partitions are reassigned.
3. A replacement worker loads the most recent valid checkpoint.
4. The worker resumes consuming from the checkpointed offsets.
5. Updated analytics are republished using idempotent output handling.

State snapshots and source offsets must be coordinated to prevent data loss or inconsistent recovery.

End-to-end exactly-once results require additional transactional or idempotent sink guarantees.

## Hybrid Cloud & Bare-Metal Execution

One of the primary goals of the project is to execute workloads across machines from different infrastructure providers.

Example cluster:

| Worker | Infrastructure | Resources |
|---|---|---|
| Worker A | AWS EC2 | 4 vCPU, 8 GB RAM |
| Worker B | GCP Compute Engine | 8 vCPU, 16 GB RAM |
| Worker C | Local Linux Server | 8 cores, 32 GB RAM |
| Worker D | Local Docker Container | 2 vCPU, 4 GB RAM |

These are example configurations, not required machine specifications.

The coordinator treats every worker through the same execution protocol, regardless of where the worker is hosted.

### Network Design

The initial deployment uses a secure private network, such as a VPN or mesh network, to allow workers across cloud providers and local machines to communicate.

Requirements:

- Authenticated coordinator-worker communication
- Secure transport using TLS
- Restricted network access
- Worker registration and identity verification
- Configurable heartbeat and connection timeouts
- Shared access to the required input and checkpoint storage

Cross-cloud network latency and data transfer costs will be measured as part of the project.

## Technology Stack

| Component | Technology |
|---|---|
| Primary Language | Kotlin 2.x |
| Distributed Coordinator | Kotlin, Coroutines |
| Worker Runtime | Kotlin, JVM |
| Network Communication | gRPC / Protocol Buffers |
| Job Scheduling | Custom Kotlin Scheduler |
| Worker Discovery | Registration + Heartbeats |
| Task Queue | Custom Coordinator-managed Queue |
| Metadata Persistence | PostgreSQL |
| Event Streaming | Apache Kafka |
| Historical Data Storage | S3 / MinIO |
| Stateful Processing | Local State / RocksDB |
| Analytical Database | ClickHouse |
| Query API | Ktor |
| Frontend | React, TypeScript |
| Visualization | Recharts / Apache ECharts |
| Build System | Gradle Kotlin DSL |
| Local Orchestration | Docker Compose |
| Cloud Deployment | AWS EC2, GCP Compute Engine |
| Bare-Metal Deployment | Linux Machines |
| Monitoring | Prometheus / Grafana |

## Project Structure

```text
real-time-analytics-system/
│
├── build.gradle.kts
├── settings.gradle.kts
├── docker-compose.yml
│
├── core/
│   ├── models/
│   ├── protocols/
│   └── serialization/
│
├── coordinator/
│   ├── api/
│   ├── job-manager/
│   ├── task-partitioner/
│   ├── scheduler/
│   ├── worker-registry/
│   ├── failure-detector/
│   ├── retry-manager/
│   └── state-store/
│
├── worker/
│   ├── registration/
│   ├── heartbeat/
│   ├── task-executor/
│   ├── resource-monitor/
│   ├── batch-runtime/
│   ├── stream-runtime/
│   └── checkpoint-manager/
│
├── batch-engine/
│   ├── input-readers/
│   ├── partition-planner/
│   ├── operators/
│   └── result-aggregator/
│
├── stream-engine/
│   ├── kafka-consumer/
│   ├── partition-assignment/
│   ├── state-management/
│   ├── checkpointing/
│   └── event-operators/
│
├── event-generator/
│
├── query-api/
│
├── dashboard-ui/
│
├── infrastructure/
│   ├── docker/
│   ├── aws/
│   ├── gcp/
│   └── bare-metal/
│
├── benchmarks/
│
└── docs/
    ├── architecture.md
    ├── scheduling.md
    ├── fault-tolerance.md
    └── execution-model.md
```

## Implementation Roadmap

### Phase 1: Build the Distributed Execution Engine

**Goal:** Execute a simple distributed workload across multiple local machines.

- [ ] Implement coordinator service in Kotlin.
- [ ] Implement worker registration.
- [ ] Implement worker heartbeats.
- [ ] Maintain an in-memory worker registry.
- [ ] Implement job submission.
- [ ] Partition jobs into independent tasks.
- [ ] Implement basic round-robin scheduling.
- [ ] Dispatch tasks using gRPC.
- [ ] Execute tasks on remote workers.
- [ ] Track task execution status.
- [ ] Combine completed task results.

**Milestone:** Submit a job containing 100 independent tasks and execute them across three local worker processes.

### Phase 2: Fault Tolerance & Recovery

**Goal:** Recover unfinished work automatically when a worker fails.

- [ ] Implement worker heartbeat timeouts.
- [ ] Detect worker crashes.
- [ ] Maintain task attempt identifiers.
- [ ] Reassign unfinished tasks.
- [ ] Implement retry policies.
- [ ] Handle stale worker responses.
- [ ] Persist job and task metadata.
- [ ] Add coordinator restart recovery.
- [ ] Implement idempotent result commits.

**Milestone:** Terminate a worker during execution and verify that the engine automatically completes the remaining job without double-counting results.

### Phase 3: Distributed Batch Processing

**Goal:** Execute analytical workloads over large historical datasets.

- [ ] Integrate S3-compatible object storage.
- [ ] Implement file-based task partitioning.
- [ ] Implement distributed COUNT and SUM operations.
- [ ] Implement grouped aggregation.
- [ ] Aggregate partial worker results.
- [ ] Write analytical outputs to ClickHouse.
- [ ] Benchmark single-worker versus multi-worker execution.

**Milestone:** Process a historical transaction dataset across multiple workers and compare performance against a single-machine baseline.

### Phase 4: Real-Time Stream Processing

**Goal:** Process live Kafka events using distributed, stateful workers.

- [ ] Integrate Kafka consumers into workers.
- [ ] Assign Kafka partitions to workers.
- [ ] Implement stateful event processing.
- [ ] Maintain per-merchant revenue state.
- [ ] Persist worker checkpoints.
- [ ] Recover from failed stream workers.
- [ ] Coordinate Kafka offsets with checkpoints.
- [ ] Publish analytical updates to ClickHouse.

**Milestone:** Continuously calculate transaction metrics while recovering correctly from simulated worker failures.

### Phase 5: Hybrid Cloud Execution

**Goal:** Execute a single distributed job across multiple infrastructure providers.

- [ ] Deploy a worker on AWS EC2.
- [ ] Deploy a worker on GCP Compute Engine.
- [ ] Deploy a worker on a bare-metal Linux machine.
- [ ] Establish secure private networking.
- [ ] Register all workers with one coordinator.
- [ ] Execute a distributed batch job across providers.
- [ ] Measure cross-cloud communication overhead.
- [ ] Implement resource-aware scheduling.

**Milestone:** Execute one job across AWS, GCP, and a local physical machine, including automated recovery from worker termination.

### Phase 6: Analytics Dashboard

**Goal:** Visualize both analytical data and distributed engine performance.

- [ ] Build the Kotlin Query API.
- [ ] Integrate ClickHouse analytical queries.
- [ ] Develop a React dashboard.
- [ ] Display live transaction metrics.
- [ ] Display active workers and resource utilization.
- [ ] Show running and completed jobs.
- [ ] Visualize task assignments.
- [ ] Display worker failures and recovery events.
- [ ] Track job completion time and throughput.

**Milestone:** Demonstrate distributed workload execution and real-time analytical updates through an interactive dashboard.

### Phase 7: Advanced Distributed Systems Features

Potential future extensions:

- [ ] DAG-based job execution.
- [ ] Multi-stage job scheduling.
- [ ] Distributed shuffle.
- [ ] Work stealing.
- [ ] Speculative execution of slow tasks.
- [ ] Data-locality-aware scheduling.
- [ ] Autoscaling workers.
- [ ] Resource quotas and admission control.
- [ ] Backpressure for streaming workloads.
- [ ] Event-time windowing and watermarking.
- [ ] Coordinated distributed checkpoints.
- [ ] High-availability coordinator.
- [ ] Cloud-cost-aware scheduling.

These features are stretch goals and are not required for the initial working engine.

## Example Distributed Execution

Suppose a user submits a job to calculate revenue from 100 million historical transactions.

### Step 1: Job Submission

```json
{
  "jobId": "revenue-job-001",
  "operation": "SUM_REVENUE",
  "input": "s3://transactions/history/",
  "partitions": 100
}
```

### Step 2: Task Partitioning

The coordinator divides the dataset into 100 independent tasks.

### Step 3: Worker Discovery

The coordinator discovers available execution resources:

```text
Worker A (AWS):        4 slots
Worker B (GCP):        8 slots
Worker C (Bare Metal): 8 slots

Total: 20 available execution slots
```

### Step 4: Scheduling

The scheduler assigns up to 20 tasks concurrently based on worker availability.

As tasks finish, additional pending tasks are scheduled.

### Step 5: Execution

Each worker reads its assigned dataset partition and computes a partial revenue aggregate.

### Step 6: Failure Recovery

If Worker B crashes:

- The coordinator detects the missing heartbeats.
- Unfinished task attempts are invalidated.
- Tasks are reassigned to healthy workers.
- Stale results from the original attempts are ignored.

### Step 7: Result Aggregation

Once all partitions have completed, their partial aggregates are combined to produce the total revenue.

The final analytical result is stored in ClickHouse and made available through the Kotlin Query API.

## Benchmarking & Performance Evaluation

The engine will be evaluated on both performance and correctness.

### Performance Metrics

- Total job execution time
- Tasks completed per second
- Worker CPU utilization
- Worker memory utilization
- Scheduler decision latency
- Task dispatch overhead
- Network transfer overhead
- Recovery time after worker failure
- Streaming event processing latency

### Experiments

**Experiment 1: Horizontal Scaling**

Compare job execution time using 1, 2, 4, and 8 workers.

**Experiment 2: Worker Failure**

Kill a worker while processing tasks and measure recovery time and output correctness.

**Experiment 3: Scheduling Strategies**

Compare round-robin and least-loaded scheduling.

**Experiment 4: Cross-Cloud Execution**

Compare execution across local workers and AWS/GCP workers, including network and data transfer overhead.

**Experiment 5: Streaming Recovery**

Interrupt a stream worker and verify that processing resumes from a consistent checkpoint without losing committed analytical results.

### Success Criteria

The engine should demonstrate:

- Correct execution of distributed jobs.
- Automatic recovery from worker failures.
- Deterministic aggregation results for supported operations.
- Improved throughput with additional workers for sufficiently parallel workloads.
- Continuous analytical processing with recoverable state.
- Successful workload execution across multiple infrastructure providers.

Performance targets will be established through empirical benchmarks rather than assumed in advance.

## Key Distributed Systems Problems Explored

### 1. Distributed Scheduling

How should tasks be assigned when workers have different CPU capacities, memory limits, network latencies, and workloads?

### 2. Fault Detection

How can the coordinator distinguish between a crashed worker and a temporarily slow or disconnected worker?

### 3. Duplicate Execution

How can the engine prevent duplicate results when an unresponsive worker is still executing a task that has already been reassigned?

### 4. Distributed State Consistency

How can stateful workers recover from crashes while maintaining consistency between persisted state and Kafka offsets?

### 5. Parallelism & Scalability

How much performance is gained by adding workers, and at what point does networking or coordination overhead dominate?

### 6. Data Locality

When is it more efficient to move computation closer to data rather than transfer large datasets between machines?

### 7. Recovery & Availability

How can the system restore useful execution progress after individual worker failures or coordinator restarts?

## Design Philosophy

The system is designed around a central principle:

**Understand distributed systems by building the mechanisms that make them work.**

Rather than treating distributed execution as a black box, this project directly implements and evaluates the mechanisms behind:

- Task partitioning
- Distributed scheduling
- Worker coordination
- Network-based execution
- Failure detection
- Task recovery
- Distributed aggregation
- Stateful stream processing

The objective is not to recreate the complete functionality or performance of Apache Spark or Apache Flink.

Instead, it is to develop a focused, working distributed execution engine that demonstrates their fundamental systems concepts and trade-offs.

## License

This project is open-source and available under the MIT License.
