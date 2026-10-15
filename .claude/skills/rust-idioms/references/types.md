# Typed values

Load when a signature, field, or constant uses `String`, `&str`, `bool`, or
a bare integer to carry meaning. Synthetic examples. Background: *The Rust
Programming Language* ch. 6 (enums, `match`), ch. 20.3 (newtype), and the
Rust API Guidelines section "Type safety".

## Enum instead of a string

```rust
// Before: every caller can typo it; every branch re-compares text.
fn render(format: &str, rows: &[Row]) -> String {
    if format == "csv" { /* ... */ } else if format == "json" { /* ... */ } else { panic!("bad format") }
}

// After: parse once at the edge, exhaustive match inside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format { Csv, Json }

impl std::str::FromStr for Format {
    type Err = UnknownFormat;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            other => Err(UnknownFormat(other.to_owned())),
        }
    }
}

fn render(format: Format, rows: &[Row]) -> String {
    match format { Format::Csv => /* ... */, Format::Json => /* ... */ }
}
```

- Derive `Debug, Clone, Copy, PartialEq, Eq, Hash` for fieldless enums
  (Book appendix C lists what each derive gives you).
- Avoid `_ =>` on enums you own. A new variant should fail to compile
  where it needs handling. Use `_ =>` only for foreign or `#[non_exhaustive]`
  types.
- A string that is also written in docs, SQL, or JSON stays a string at the
  edge; give it one `as_str()` and one `FromStr`, and nothing else spells it.

## Newtype for ids and units

```rust
struct UserId(u64);
struct OrderId(u64);
fn cancel(order: OrderId, by: UserId) { /* swapping the args no longer compiles */ }
```

Add `#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]`; implement
`From`/`TryFrom` for the conversions you need rather than exposing `.0`.
Skip the newtype when only one value of that primitive exists in the
function or the value never leaves one module.

## Options and flags

- `Option<T>` for absence; `""`, `0`, `-1`, `usize::MAX` as "none" is a
  bug waiting for a caller that passes the real value.
- `fn run(dry_run: bool, verbose: bool)` is unreadable at the call site:
  use two small enums or an options struct.
- Two fields that are only valid together (`Option<A>`, `Option<B>`): one
  `Option<(A, B)>` or a struct, so half-set state cannot exist.

## Constants

Repeated literal (a limit, a key, a file name): one `const NAME: T` next to
its owner, referenced everywhere, including tests. Use `const` arrays or
`match` for lookup tables instead of `if` chains.

## Visibility

Default to private, then `pub(crate)`, then `pub`. A field made `pub` to
satisfy one caller is usually a missing method or a missing constructor
that checks the invariant. Make invalid states unrepresentable instead of
documenting them.

## Pattern helpers

- `let Some(x) = opt else { return Err(..) };` flattens nested `if let`.
- `matches!(v, A | B)` for yes/no checks on variants.
- Destructure in the signature or `let` rather than indexing `t.0`, `t.1`.
