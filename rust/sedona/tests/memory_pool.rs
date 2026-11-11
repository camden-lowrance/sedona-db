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

use std::sync::{mpsc, Arc, Barrier};

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use sedona::memory_pool::SedonaFairSpillPool;

#[test]
fn idle_spillers_do_not_reduce_available_memory() {
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(100, 0.2));
    let active = MemoryConsumer::new("active")
        .with_can_spill(true)
        .register(&pool);
    let _idle: Vec<_> = (0..128)
        .map(|i| {
            MemoryConsumer::new(format!("idle-{i}"))
                .with_can_spill(true)
                .register(&pool)
        })
        .collect();

    active.try_grow(80).unwrap();
    assert!(active.try_grow(1).is_err());
    assert_eq!(pool.reserved(), 80);
}

#[test]
fn split_reservations_share_the_consumers_limit() {
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(100, 0.0));
    let first = MemoryConsumer::new("first")
        .with_can_spill(true)
        .register(&pool);
    let second = MemoryConsumer::new("second")
        .with_can_spill(true)
        .register(&pool);
    first.try_grow(50).unwrap();
    second.try_grow(50).unwrap();
    let split = first.split(25);

    assert!(first.try_grow(25).is_err());
    assert_eq!(pool.reserved(), 100);

    drop(split);
    assert_eq!(pool.reserved(), 75);
    first.try_grow(25).unwrap();
    assert_eq!(pool.reserved(), 100);
}

#[test]
fn late_spiller_cannot_exceed_the_pool_limit() {
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(100, 0.0));
    let first = MemoryConsumer::new("first")
        .with_can_spill(true)
        .register(&pool);
    first.try_grow(100).unwrap();
    let second = MemoryConsumer::new("second")
        .with_can_spill(true)
        .register(&pool);

    assert!(second.try_grow(1).is_err());
    assert_eq!(pool.reserved(), 100);
    first.shrink(50);
    second.try_grow(50).unwrap();
    assert_eq!(pool.reserved(), 100);
}

#[test]
fn later_spiller_can_progress_after_memory_is_released() {
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(100, 0.2));
    let first = MemoryConsumer::new("first")
        .with_can_spill(true)
        .register(&pool);
    first.try_grow(80).unwrap();
    let second = MemoryConsumer::new("second")
        .with_can_spill(true)
        .register(&pool);
    let (attempted_tx, attempted_rx) = mpsc::channel();
    let (released_tx, released_rx) = mpsc::channel();

    std::thread::scope(|scope| {
        let handle = scope.spawn(move || {
            attempted_tx.send(second.try_grow(40).is_err()).unwrap();
            released_rx.recv().unwrap();
            second.try_grow(40).is_ok()
        });

        let blocked = attempted_rx.recv().unwrap();
        first.shrink(40);
        released_tx.send(()).unwrap();
        let progressed = handle.join().unwrap();
        assert!(blocked);
        assert!(progressed);
    });
    assert_eq!(pool.reserved(), 40);
}

#[test]
fn dropping_split_reservations_releases_the_consumer() {
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(100, 0.0));
    let first = MemoryConsumer::new("first")
        .with_can_spill(true)
        .register(&pool);
    first.try_grow(60).unwrap();
    let split = first.split(30);
    drop(first);

    let second = MemoryConsumer::new("second")
        .with_can_spill(true)
        .register(&pool);
    second.try_grow(50).unwrap();
    assert!(second.try_grow(1).is_err());
    drop(split);
    second.try_grow(50).unwrap();
    assert_eq!(pool.reserved(), 100);
}

#[test]
fn concurrent_spillers_do_not_exceed_the_pool_limit() {
    let pool: Arc<dyn MemoryPool> = Arc::new(SedonaFairSpillPool::new(100, 0.0));
    let barrier = Arc::new(Barrier::new(10));
    let reservations = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let pool = pool.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    let reservation = MemoryConsumer::new(format!("consumer-{i}"))
                        .with_can_spill(true)
                        .register(&pool);
                    barrier.wait();
                    for _ in 0..20 {
                        if reservation.try_grow(1).is_err() {
                            break;
                        }
                    }
                    reservation
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });

    let total: usize = reservations.iter().map(|r| r.size()).sum();
    assert!(total <= 100);
    assert_eq!(pool.reserved(), total);
    drop(reservations);
    assert_eq!(pool.reserved(), 0);
}
