# flow-engine

Copyright (C) 2022-2023 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction

This is a simple flow engine that can be used to execute a series of tasks in a specific order. The tasks are executed in a separate thread. The tasks are executed in the order they are added to the flow. The flow can be executed multiple times. The flow can be executed in parallel or in sequence.

## Custom Executors for Parallel Stages
### Use Case

This is particularly useful when you need to:
- Limit the number of concurrent operations for resource-intensive tasks
- Control the thread pool size for specific operations (e.g., module installations, database operations)
- Isolate parallel execution environments for different types of tasks
- Apply different threading strategies to different parts of your workflow

### Usage Examples

#### Basic Usage with Custom Executor

```java
import java.util.concurrent.Executors;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.Flow;

// Create a custom executor with limited threads
var customExecutor = Executors.newFixedThreadPool(4);

// Create a parallel stage with custom executor
var parallelStage = ParallelStage.of(
    "module-installer-stage",
    List.of(stage1, stage2, stage3, stage4, stage5),
    customExecutor
);

// Use in flow
var flow = Flow.builder()
    .stage(parallelStage)
    .build();
```

#### Using Builder Pattern

```java
var customExecutor = Executors.newFixedThreadPool(4);

var parallelStage = ParallelStage.parallelStageBuilder()
    .id("custom-stage")
    .stage(stage1)
    .stage(stage2)
    .stage(stage3)
    .executor(customExecutor)
    .build();
```

#### Without Custom Executor (Default Behavior)

```java
// Uses the FlowEngine's default executor
var parallelStage = ParallelStage.of(
    "default-stage",
    List.of(stage1, stage2, stage3)
);
```

### Integration with mgr-tenant-entitlements

This feature enables the `FLOW_ENGINE_MODULE_INSTALLER_THREADS` environment variable in mgr-tenant-entitlements to control the number of modules being installed in parallel, independently from the main `FLOW_ENGINE_THREADS_NUM` setting.

Example configuration in mgr-tenant-entitlements:
```yaml
flow-engine:
  module-installer-threads: ${FLOW_ENGINE_MODULE_INSTALLER_THREADS:4}
```

Then use it to create parallel stages:
```java
@Autowired
private Executor moduleInstallerExecutor;

var moduleInstallerStage = ParallelStage.of(
    "folio-module-installer",
    moduleInstallerStages,
    moduleInstallerExecutor
);
```

### Important Notes

- If no custom executor is provided, the parallel stage uses the default executor from the FlowEngine
- Custom executors are used for the execution, skip, and cancel operations of the parallel stage
- Nested parallel stages can have different custom executors
- The custom executor should be properly managed (shutdown) by the application
- Thread pool size should be configured based on available resources and workload characteristics

### API Reference

**ParallelStage.of(String id, List<Stage> stages, Executor customExecutor)**
- Creates a parallel stage with a custom executor
- Parameters:
  - `id`: Stage identifier
  - `stages`: List of stages to execute in parallel
  - `customExecutor`: Custom executor for parallel execution
- Returns: ParallelStage instance

**ParallelStageBuilder.executor(Executor customExecutor)**
- Sets custom executor in builder pattern
- Parameters:
  - `customExecutor`: Custom executor for parallel execution
- Returns: ParallelStageBuilder instance (for chaining)