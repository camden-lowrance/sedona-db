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

This page describes the preferred pattern for adding scalar SQL functions to
SedonaDB. Existing functions predate this guide and may not all follow every
point below.

## Placement and registration

Most scalar functions are implemented in `rust/sedona-functions/src` as one
`st_function_name.rs` file per SQL function or closely related function family.
Functions that depend on optional geometry engines, such as `geo`, GEOS, or S2,
may live in the crate that owns that dependency.

Register new scalar functions in the crate's function set, typically
`rust/sedona-functions/src/register.rs`, so that they are available from SQL.

Add user-facing SQL documentation in `docs/reference/sql/function_name.qmd`.

## UDF shape

Expose a constructor named after the SQL function:

```rust
pub fn st_function_name_udf() -> SedonaScalarUDF
```

Use lowercase SQL names when constructing `SedonaScalarUDF`. Add aliases with
`with_aliases()` instead of registering duplicate implementations.

Implement the kernel as a small `#[derive(Debug)]` struct that implements
`SedonaScalarKernel`. Keep SQL registration, type matching, batch execution, and
per-geometry helper logic separate enough that each part can be tested directly.

When the function should preserve item CRS metadata from its input, wrap kernels
with `ItemCrsKernel::wrap_impl(...)`.

## Type matching

Use `ArgMatcher` in `return_type()` for ordinary signatures. Return `Ok(None)`
for unsupported signatures instead of raising an execution error during planning.

Be explicit about geometry and geography support. If a function supports both,
prefer separate matchers or kernels where that keeps return metadata clear.

Preserve view-backed storage when that is the semantic result. For example,
pass-through functions on `WkbView` inputs should avoid forcing `Binary` output
unless the function really rewrites the geometry bytes.

## Batch execution

Use the executor helpers in `rust/sedona-functions/src/executor.rs` instead of
manually expanding scalar and array arguments. `WkbExecutor` is the usual choice
when the implementation needs parsed geometry access. `WkbBytesExecutor` is a
better fit when the function can inspect WKB bytes directly.

Allocate Arrow builders with `executor.num_iterations()` and a reasonable value
capacity. Append nulls explicitly when input rows are null. Finish output through
`executor.finish(...)` so scalar inputs and array inputs both produce the right
`ColumnarValue`.

Avoid `unwrap()` and `expect()` in runtime paths. Use DataFusion errors for user
or data failures, and `sedona_internal_err!` for invariants that indicate a bug
in SedonaDB.

## Tests

Add focused Rust tests with `ScalarUdfTester` for:

- UDF metadata, including the SQL name and aliases.
- Scalar input, array input, and null handling.
- Geometry and geography signatures where both are supported.
- Unsupported signatures when the function has narrow type support.
- Storage-specific behavior, such as `WkbView` or CRS metadata preservation.

Prefer small, readable WKT examples. Add regression tests for edge cases that
motivated the function or implementation choice.

## Documentation checklist

Before opening a pull request:

- Add or update the scalar implementation and registration.
- Add Rust tests for scalar and array execution.
- Add `docs/reference/sql/function_name.qmd`.
- Run the relevant Rust tests.
- Run `quarto render` in `docs/reference/sql` if SQL function docs changed.
- Run `mkdocs build` if contributor or site navigation docs changed.
