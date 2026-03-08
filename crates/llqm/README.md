# llqm

Query IR with an LLVM-inspired typed pipeline (`Frontend -> IR passes -> Backend -> Emit passes`).

Substrait is only touched in `ir/substrait.rs` for DataFusion encode/decode.
Everything else works with the `Rel`/`Expr` tree directly.

## Future work

- `read_raw` (raw FROM clauses) is a stopgap. Long-term, table functions and
  inline subselects should be first-class `Rel` variants instead of a sentinel
  tag on `ReadRel`.
