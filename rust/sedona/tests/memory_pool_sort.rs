// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use sedona::memory_pool::SedonaFairSpillPool;

#[tokio::test]
async fn idle_sorts_do_not_force_active_sort_to_spill() {
    // Blocking sorts register before the first sort consumes its input.
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batches: Vec<_> = (0..100)
        .map(|i| {
            let keys: Vec<i64> = (0..256).rev().map(|k| i * 256 + k).collect();
            let values: Vec<i64> = (0..256).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap()
        })
        .collect();
    let mut plan: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[batches], schema.clone(), None).unwrap();
    let mut sorts = Vec::new();
    for i in 0..5 {
        let field = if i % 2 == 0 { "key" } else { "value" };
        let ordering = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col(field, &schema).unwrap(),
            options: SortOptions::default(),
        }])
        .unwrap();
        let sort = Arc::new(SortExec::new(ordering, plan));
        sorts.push(sort.clone());
        plan = sort;
    }

    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(1_400_000, 0.2));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    let config = SessionConfig::new()
        .with_batch_size(256)
        .with_sort_spill_reservation_bytes(65_536);
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    );
    let mut stream = plan.execute(0, context).unwrap();
    let mut keys = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        keys.extend_from_slice(column.values());
    }

    assert_eq!(keys, (0..25_600).collect::<Vec<_>>());
    assert_eq!(sorts[0].metrics().unwrap().spill_count(), Some(0));
}

#[tokio::test]
async fn partitioned_sorts_complete_and_release_memory() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let partitions: Vec<Vec<RecordBatch>> = (0..4)
        .map(|partition| {
            (0..25)
                .map(|batch| {
                    let keys: Vec<i64> = (0..256)
                        .rev()
                        .map(|key| partition * 6_400 + batch * 256 + key)
                        .collect();
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(keys))])
                        .unwrap()
                })
                .collect()
        })
        .collect();
    let input = MemorySourceConfig::try_new_exec(&partitions, schema.clone(), None).unwrap();
    let ordering = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("key", &schema).unwrap(),
        options: SortOptions::default(),
    }])
    .unwrap();
    let sort = SortExec::new(ordering, input).with_preserve_partitioning(true);
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(1_400_000, 0.2));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let config = SessionConfig::new()
        .with_batch_size(256)
        .with_sort_spill_reservation_bytes(65_536);
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    );
    let streams: Vec<_> = (0..4)
        .map(|partition| sort.execute(partition, context.clone()).unwrap())
        .collect();
    let results = futures::future::join_all(streams.into_iter().map(|mut stream| async move {
        let mut keys = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            keys.extend_from_slice(column.values());
        }
        keys
    }))
    .await;

    for (partition, keys) in results.iter().enumerate() {
        let start = partition as i64 * 6_400;
        assert_eq!(*keys, (start..start + 6_400).collect::<Vec<_>>());
    }
    assert_eq!(pool.reserved(), 0);
}
