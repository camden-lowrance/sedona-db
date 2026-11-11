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

#![cfg(all(feature = "spatial-join", feature = "geos"))]

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::MemTable;
use datafusion::physical_plan::displayable;
use sedona::context_builder::SedonaContextBuilder;
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_testing::create::create_array;

fn partitioned_geometry_table(polygons: bool) -> MemTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        WKB_GEOMETRY.to_storage_field("geom", false).unwrap(),
    ]));
    let partitions = (0..4)
        .map(|partition| {
            let ids: Vec<i32> = (partition * 10..partition * 10 + 10).collect();
            let wkts: Vec<String> = ids
                .iter()
                .map(|id| {
                    let x = id * 2;
                    if polygons {
                        format!("POLYGON (({x} 0, {} 0, {} 1, {x} 1, {x} 0))", x + 1, x + 1)
                    } else {
                        format!("POINT ({}.5 0.5)", x)
                    }
                })
                .collect();
            let values: Vec<_> = wkts.iter().map(|wkt| Some(wkt.as_str())).collect();
            let geometries = create_array(&values, &WKB_GEOMETRY);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(ids)), geometries],
            )
            .unwrap();
            vec![batch]
        })
        .collect();
    MemTable::try_new(schema, partitions).unwrap()
}

#[tokio::test]
async fn partitioned_spatial_join_completes_with_a_memory_limit() {
    let ctx = SedonaContextBuilder::new()
        .with_memory_limit(128 * 1024 * 1024)
        .build()
        .await
        .unwrap();
    ctx.ctx
        .register_table("polygons", Arc::new(partitioned_geometry_table(true)))
        .unwrap();
    ctx.ctx
        .register_table("points", Arc::new(partitioned_geometry_table(false)))
        .unwrap();
    let df = ctx
        .sql(
            "SELECT points.id AS point_id, polygons.id AS polygon_id \
             FROM points JOIN polygons ON ST_Intersects(points.geom, polygons.geom) \
             ORDER BY points.id",
        )
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    assert!(plan.contains("SpatialJoinExec"), "{plan}");

    let mut rows = Vec::new();
    for batch in df.collect().await.unwrap() {
        let points = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let polygons = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend(
            points
                .values()
                .iter()
                .copied()
                .zip(polygons.values().iter().copied()),
        );
    }
    assert_eq!(rows, (0..40).map(|id| (id, id)).collect::<Vec<_>>());
    assert_eq!(ctx.ctx.runtime_env().memory_pool.reserved(), 0);
}
