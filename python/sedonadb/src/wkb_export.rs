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

use std::{borrow::Cow, sync::Arc};

use arrow_array::{
    builder::GenericBinaryBuilder, Array, ArrayRef, GenericBinaryArray, OffsetSizeTrait,
    RecordBatch, RecordBatchIterator, RecordBatchReader,
};
use arrow_schema::{ArrowError, DataType};
use datafusion_common::Result;

type WkbResult<T> = std::result::Result<T, ArrowError>;

const EWKB_Z: u32 = 0x8000_0000;
const EWKB_M: u32 = 0x4000_0000;
const EWKB_SRID: u32 = 0x2000_0000;
const EWKB_FLAGS: u32 = EWKB_Z | EWKB_M | EWKB_SRID;

// Preserve coordinate bytes and each nested geometry's byte order while
// rewriting only EWKB headers.
struct WkbCursor<'a> {
    input: &'a [u8],
    offset: usize,
    output: Option<Vec<u8>>,
    changed: bool,
}

impl<'a> WkbCursor<'a> {
    fn take(&mut self, size: usize) -> WkbResult<&'a [u8]> {
        let end = self.offset.checked_add(size).ok_or_else(|| {
            ArrowError::ParseError("WKB size exceeds addressable memory".to_string())
        })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or_else(|| ArrowError::ParseError("WKB geometry is truncated".to_string()))?;
        self.offset = end;
        Ok(value)
    }

    fn copy(&mut self, size: usize) -> WkbResult<()> {
        let value = self.take(size)?;
        if let Some(output) = &mut self.output {
            output.extend_from_slice(value);
        }
        Ok(())
    }

    fn count(&mut self, little_endian: bool) -> WkbResult<usize> {
        let value: [u8; 4] = self.take(4)?.try_into().unwrap();
        if let Some(output) = &mut self.output {
            output.extend_from_slice(&value);
        }
        Ok(if little_endian {
            u32::from_le_bytes(value)
        } else {
            u32::from_be_bytes(value)
        } as usize)
    }

    fn coordinates(&mut self, count: usize, dimensions: usize) -> WkbResult<()> {
        let size = count
            .checked_mul(dimensions)
            .and_then(|n| n.checked_mul(8))
            .ok_or_else(|| ArrowError::ParseError("WKB coordinate size overflows".to_string()))?;
        self.copy(size)
    }

    fn geometry(&mut self, depth: usize) -> WkbResult<()> {
        if depth > 128 {
            return Err(ArrowError::ParseError(
                "WKB geometry nesting exceeds 128 levels".to_string(),
            ));
        }
        let order = self.take(1)?[0];
        let little_endian = match order {
            0 => false,
            1 => true,
            _ => return Err(ArrowError::ParseError("Invalid WKB byte order".to_string())),
        };
        let raw: [u8; 4] = self.take(4)?.try_into().unwrap();
        let code = if little_endian {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        };
        if code & 0x1000_0000 != 0 {
            return Err(ArrowError::ParseError("Unsupported EWKB flag".to_string()));
        }
        let iso_code = code & 0x0fff_ffff;
        let base = iso_code % 1000;
        let iso_dimensions = iso_code / 1000;
        if !matches!(base, 1..=12 | 15..=17) || iso_dimensions > 3 {
            return Err(ArrowError::ParseError(format!(
                "Unsupported WKB geometry type: {code}"
            )));
        }
        let flag_dimensions = u32::from(code & EWKB_Z != 0) + 2 * u32::from(code & EWKB_M != 0);
        if flag_dimensions != 0 && iso_dimensions != 0 && flag_dimensions != iso_dimensions {
            return Err(ArrowError::ParseError(
                "Conflicting WKB dimensions".to_string(),
            ));
        }
        let dimension_code = iso_dimensions.max(flag_dimensions);
        let dimensions =
            2 + usize::from(dimension_code & 1 != 0) + usize::from(dimension_code & 2 != 0);
        let normalized_code = base + 1000 * dimension_code;
        if code & EWKB_FLAGS != 0 {
            self.changed = true;
        }
        if let Some(output) = &mut self.output {
            output.push(order);
            output.extend_from_slice(&if little_endian {
                normalized_code.to_le_bytes()
            } else {
                normalized_code.to_be_bytes()
            });
        }
        if code & EWKB_SRID != 0 {
            self.take(4)?;
        }

        match base {
            1 => self.coordinates(1, dimensions)?,
            2 | 8 => {
                let count = self.count(little_endian)?;
                self.coordinates(count, dimensions)?;
            }
            3 | 17 => {
                let rings = self.count(little_endian)?;
                for _ in 0..rings {
                    let count = self.count(little_endian)?;
                    self.coordinates(count, dimensions)?;
                }
            }
            4..=7 | 9..=12 | 15..=16 => {
                let count = self.count(little_endian)?;
                for _ in 0..count {
                    self.geometry(depth + 1)?;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

/// Whether WKB may contain EWKB flags or an embedded SRID.
fn needs_normalization(bytes: &[u8]) -> WkbResult<bool> {
    let header: [u8; 4] = bytes
        .get(1..5)
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| ArrowError::ParseError("WKB header is truncated".to_string()))?;
    let code = match bytes[0] {
        0 => u32::from_be_bytes(header),
        1 => u32::from_le_bytes(header),
        _ => return Err(ArrowError::ParseError("Invalid WKB byte order".to_string())),
    };

    let base_type = (code & 0x0fff_ffff) % 1000;
    if code & 0xf000_0000 == 0 && !matches!(base_type, 4..=7 | 9..=12 | 15..=16) {
        return Ok(false);
    }

    let mut cursor = WkbCursor {
        input: bytes,
        offset: 0,
        output: None,
        changed: false,
    };
    cursor.geometry(0)?;
    if cursor.changed && cursor.offset != bytes.len() {
        return Err(ArrowError::ParseError(
            "Trailing bytes after EWKB geometry".to_string(),
        ));
    }
    Ok(cursor.changed)
}

/// Convert EWKB to ISO WKB without changing the geometry's coordinates.
fn normalize_wkb(bytes: &[u8]) -> WkbResult<Cow<'_, [u8]>> {
    if !needs_normalization(bytes)? {
        return Ok(Cow::Borrowed(bytes));
    }

    let mut cursor = WkbCursor {
        input: bytes,
        offset: 0,
        output: Some(Vec::with_capacity(bytes.len())),
        changed: false,
    };
    cursor.geometry(0)?;
    Ok(Cow::Owned(cursor.output.unwrap()))
}

/// Normalize the geometry column without changing batch order or schema metadata.
pub(crate) fn normalize_wkb_reader(
    reader: Box<dyn RecordBatchReader + Send>,
    geometry_name: &str,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    let schema = reader.schema();
    let index = schema.index_of(geometry_name)?;
    if !matches!(
        schema.field(index).data_type(),
        DataType::Binary | DataType::LargeBinary
    ) {
        return Ok(reader);
    }

    let iter_schema = schema.clone();
    let iter = reader.map(move |batch| {
        let batch = batch?;
        let column = batch.column(index);
        let normalized = match column.data_type() {
            DataType::Binary => normalize_array(
                column
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i32>>()
                    .unwrap(),
            )?,
            DataType::LargeBinary => normalize_array(
                column
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i64>>()
                    .unwrap(),
            )?,
            _ => unreachable!(),
        };
        if let Some(column) = normalized {
            let mut columns = batch.columns().to_vec();
            columns[index] = column;
            RecordBatch::try_new(iter_schema.clone(), columns)
        } else {
            Ok(batch)
        }
    });
    Ok(Box::new(RecordBatchIterator::new(iter, schema)))
}

fn normalize_array<O: OffsetSizeTrait>(
    array: &GenericBinaryArray<O>,
) -> std::result::Result<Option<ArrayRef>, ArrowError> {
    let mut needs_work = false;
    for bytes in array.iter().flatten() {
        if needs_normalization(bytes)? {
            needs_work = true;
            break;
        }
    }
    if !needs_work {
        return Ok(None);
    }

    let mut builder =
        GenericBinaryBuilder::<O>::with_capacity(array.len(), array.value_data().len());
    for value in array.iter() {
        match value {
            Some(bytes) => {
                let normalized = normalize_wkb(bytes)?;
                builder.append_value(normalized.as_ref());
            }
            None => builder.append_null(),
        }
    }
    Ok(Some(Arc::new(builder.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_embedded_srid() {
        let ewkb = [
            1, 1, 0, 0, 0x20, 0xad, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0,
            0x3f,
        ];
        let expected = [
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f,
        ];
        assert_eq!(normalize_wkb(&ewkb).unwrap().as_ref(), &expected);
        assert!(matches!(
            normalize_wkb(&expected).unwrap(),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn normalizes_nested_ewkb() {
        let mut collection = vec![1, 7, 0, 0, 0, 1, 0, 0, 0];
        collection.extend_from_slice(&[
            1, 1, 0, 0, 0x20, 0xad, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0,
            0x3f,
        ]);
        let mut expected = vec![1, 7, 0, 0, 0, 1, 0, 0, 0];
        expected.extend_from_slice(&[
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f,
        ]);
        assert_eq!(normalize_wkb(&collection).unwrap().as_ref(), &expected);
    }

    #[test]
    fn preserves_coordinate_bytes_for_dimensions_and_byte_orders() {
        for little_endian in [false, true] {
            for (flags, dimension_code, dimensions) in [
                (EWKB_Z, 1000, 3),
                (EWKB_M, 2000, 3),
                (EWKB_Z | EWKB_M, 3000, 4),
            ] {
                let u32_bytes = |value: u32| {
                    if little_endian {
                        value.to_le_bytes()
                    } else {
                        value.to_be_bytes()
                    }
                };
                let mut ewkb = vec![u8::from(little_endian)];
                ewkb.extend_from_slice(&u32_bytes(1 | flags | EWKB_SRID));
                ewkb.extend_from_slice(&u32_bytes(4269));
                let mut expected = vec![u8::from(little_endian)];
                expected.extend_from_slice(&u32_bytes(1 + dimension_code));
                for value in 1..=dimensions {
                    let coordinate = if little_endian {
                        (value as f64).to_le_bytes()
                    } else {
                        (value as f64).to_be_bytes()
                    };
                    ewkb.extend_from_slice(&coordinate);
                    expected.extend_from_slice(&coordinate);
                }
                assert_eq!(normalize_wkb(&ewkb).unwrap().as_ref(), &expected);
            }
        }
    }

    #[test]
    fn rejects_truncated_or_ambiguous_ewkb() {
        let truncated = [1, 1, 0, 0, 0x20, 0xad, 0x10, 0, 0];
        assert!(normalize_wkb(&truncated).is_err());

        let unsupported_flag = [1, 1, 0, 0, 0x10];
        assert!(normalize_wkb(&unsupported_flag).is_err());
    }
}
