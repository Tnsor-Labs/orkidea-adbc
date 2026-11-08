# Type Support

## Rust ADBC Driver

Oracle metadata is mapped to Arrow using the native `oracledb` Arrow path.
Current mappings include:

- Character data: `Utf8` and `LargeUtf8`
- Binary data: `Binary` and `LargeBinary`
- Dates and timestamps: Arrow timestamp values
- `BINARY_FLOAT` and `BINARY_DOUBLE`: `Float32` and `Float64`
- `BOOLEAN`: `Boolean`
- `NUMBER`: integral Arrow types, `Float64`, or `Decimal128` based on precision

Unsupported Oracle types return an explicit error and can generally be handled
by casting in SQL.

## DuckDB Integration

The DuckDB table function maps primitive Arrow values, strings, large strings,
binary values, timestamps, and decimals to DuckDB vectors. Oracle `NUMBER`
values may appear as `DOUBLE` when Oracle metadata does not provide a bounded
precision and scale.
