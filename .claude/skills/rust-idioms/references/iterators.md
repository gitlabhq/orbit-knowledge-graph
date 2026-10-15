# Iterators and allocation

Load when the code builds `Vec`s, loops with indices, clones data, or
returns collections. Background: *The Rust Programming Language* ch. 13.2
and 13.4 (iterators are zero-cost, so write for clarity) and ch. 4.
Synthetic examples.

## Return and accept iterators

```rust
// Before: allocates a Vec the caller only loops over once.
fn active_names(users: &[User]) -> Vec<String> {
    users.iter().filter(|u| u.active).map(|u| u.name.clone()).collect()
}

// After: lazy, borrows, caller decides whether to collect.
fn active_names(users: &[User]) -> impl Iterator<Item = &str> {
    users.iter().filter(|u| u.active).map(|u| u.name.as_str())
}
```

- Keep the `Vec` when the caller needs the length, random access, or to
  keep the data after the source goes away. Say so in the return type.
- Take `impl IntoIterator<Item = T>` when you only loop; `&[T]` when you
  need slicing or length.
- Hand back a borrowed `&str`/`&T` from the iterator; let callers
  `to_owned()` if they need to store it.

## One pass

- Chain `filter`, `map`, `flat_map`, `take_while` and `collect` once. Two
  intermediate `collect()`s to split or zip data is a missing struct or a
  single `fold`/loop that builds both results.
- `iter().map(f).collect::<Result<Vec<_>, _>>()?` and
  `.sum::<Option<_>>()` stop at the first failure; do not loop and push.
- Prefer `find`, `any`, `all`, `position`, `max_by_key` over a hand loop
  with a flag. Keep a `for` loop when the body has side effects, early
  `return`s, or `?`; a loop is not a smell by itself.
- Use `enumerate()`, `zip()`, `windows(2)`, `chunks(n)` when positions or
  neighbours matter.

## Do not scan repeatedly

```rust
// Before: O(n * m)
for item in &items { if allowed.contains(&item.id) { /* ... */ } }   // allowed: Vec<Id>

// After: build the set once.
let allowed: HashSet<Id> = allowed.into_iter().collect();
```

- Many lookups into the same data: build a `HashMap`/`HashSet`/`BTreeMap`
  once. Sorted data plus `binary_search` also works.
- Inside a loop, move invariants out: compile the regex, build the set, and
  format the prefix once.

## Allocation

- Call `.to_owned()`/`.to_string()` only at the point where something must
  own the value.
- `collect::<Vec<_>>()` from an iterator with an exact size already
  reserves. `Vec::with_capacity(n)` when you push in a loop and know `n`.
- `clone()` inside a loop or on a large value: first try borrowing,
  reordering the borrow, `Rc`/`Arc` for genuinely shared data, or moving.
  `mem::take`/`mem::replace`/`Option::take` swap a field out without
  cloning it.
- `String` building: `write!(buf, ..)` into one buffer, or `join`, instead
  of `format!` pieces concatenated with `+`.
- Optional values do not need a `Vec`: `Option<T>` for zero or one.
- Small fixed collections (`SmallVec`, arrays) are a measured optimisation,
  not a default. Write the plain version first.

## Sorting and dedup

`sort_unstable_by_key` when stability is not needed; `dedup`/`dedup_by_key`
after a sort; `BTreeSet` when you need sorted unique data throughout.
