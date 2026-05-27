<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Scalar Function Style Guide

This page describes the preferred scalar function pattern for arguments that can
be either scalar or array values.

## Scalar-or-array arguments

Use the executor helpers in `rust/sedona-functions/src/executor.rs` instead of
manually branching on scalar and array arguments. For non-geometry arguments,
cast the argument once, convert it to an array with `executor.num_iterations()`,
iterate it in lockstep with the geometry executor, and handle nulls in the row
loop:

```rust
let arg1 = args[1]
    .cast_to(&DataType::Float64, None)?
    .to_array(executor.num_iterations())?;
let arg1_array = as_float64_array(&arg1)?;
let mut arg1_iter = arg1_array.iter();

executor.execute_wkb_void(|maybe_wkb| {
    match (maybe_wkb, arg1_iter.next().unwrap()) {
        (Some(wkb), Some(arg1)) => {
            invoke_scalar(&wkb, arg1, &mut builder)?;
            builder.append_value([]);
        }
        _ => builder.append_null(),
    }

    Ok(())
})?;
```

Avoid `unwrap()` and `expect()` in runtime paths. Use DataFusion errors for user
or data failures, and `sedona_internal_err!` for invariants that indicate a bug
in SedonaDB.

## Tests

When a scalar function accepts both scalar and array arguments, add
`ScalarUdfTester` coverage for both paths and for null handling.
