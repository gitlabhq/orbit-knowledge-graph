# Standard names in code indexing

Language lists live in `crates/code-graph-incremental/langs/*.yaml`.
They describe lookup policy, not a complete API inventory or a runtime emulator.

## Configuration

`config.stdlib` is a list of provider objects consumed by linking and resolution.
Each entry has an optional module, symbol list, availability, and module precedence.

```yaml
config:
  stdlib:
    - module: stdio.h
      symbols: [printf, puts]
    - module: time
      precedence: runtime
    - module: builtins
      precedence: runtime
      availability: implicit
      symbols: [len, print]
    - module: json
    - symbols: ['@sizeOf', '@TypeOf']
      availability: implicit
```

`symbols` applies only to an import of that exact canonical module path. These
names suppress speculative wildcard-call fallback for that provider, while concrete
project definitions still resolve. A spelling such as `printf` is not implicitly
builtin merely because one standard header supplies it.

`availability: implicit` makes symbols available without an import. The default
is `imported`. Implicit symbols without a module describe reserved operations or
globals, including Bash commands, Zig `@` functions, and PHP/JavaScript globals.

`precedence: runtime` selects the runtime module before ordinary project lookup.
Explicit project aliases are applied before either module classification. The
same applies to successful bare-import lookups under an explicitly configured
`baseUrl`. Relative paths and scheme-qualified imports such as `node:constants`
do not use that override. Inferred directory roots cannot override runtime modules.
Changing or removing configured roots invalidates retained import targets; adding
roots also rediscovers imports that previously had no project target.
Configured roots compile to conditional wildcard aliases. They share ordered
alias lookup with explicit mappings, which take priority. A conditional mapping
applies only when its candidate exists. One lookup configuration tracks changes
to aliases and search prefixes and is reconstructed when loading a snapshot.
The default, `precedence: project`, retains exact and declared-root project lookup.
Provider lists are compiled once when language rules load. They do not add a
new resolution pass or inspect language-specific syntax in the shared engine.

Module entries match a canonical module path and its slash-separated children.
For example, `java/lang` matches `java/lang/Math`, but not `java/language`.
Symbol associations match the exact provider path. Language YAML
normalizes namespace separators before this check. The original canonical path
is retained across resolution and snapshots, even when the resolved path changes.

Snapshot version 15 requires a fresh index for older snapshots, which lack that
original-path metadata.

Every entry needs a module or nonempty implicit symbols. Availability requires
symbols; precedence requires a module. Empty names, unknown fields, unknown policy
values, and bare-string entries are rejected. The old `link.builtins`,
`resolve.external`, and `resolve.stdlib` fields are no longer accepted.

## Per-language coverage

The YAML files hold the enumerated lists. This table explains their scope and
links to language references for maintaining them.

| Language | Builtin callable names | Module classification | Reference |
|---|---|---|---|
| Bash | Shell builtins such as `printf`, `read`, `cd`, and `declare` | No reserved source-file roots | [Bash builtins](https://www.gnu.org/software/bash/manual/html_node/Shell-Builtin-Commands.html) |
| C | No implicit library functions; symbols belong to header providers | Standard headers use `stdlib`; project headers remain eligible | [C library](https://en.cppreference.com/w/c/header.html) |
| C++ | No implicit library functions; symbols belong to header providers | Standard headers use `stdlib` | [C++ headers](https://en.cppreference.com/w/cpp/header.html) |
| C# | Empty: no implicit global standard functions | `System` uses `stdlib` | [Namespaces](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/namespace) |
| Elixir | Implicit Kernel provider | Core roots plus Bitwise and Enum symbol providers | [Kernel](https://hexdocs.pm/elixir/Kernel.html) |
| Go | Implicit `builtin` provider | Runtime package roots plus fmt/errors symbol providers | [Builtins](https://pkg.go.dev/builtin), [packages](https://pkg.go.dev/std) |
| Java | Implicit `java/lang` provider | Math and Objects static providers; `java` is external | [java.lang](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/package-summary.html) |
| Kotlin | Implicit kotlin, io, collections, and sequences providers | Explicit math provider; runtime kotlin/java roots | [Default imports](https://kotlinlang.org/docs/packages.html#default-imports) |
| Lua | Global base-library functions | Normally preloaded modules use runtime precedence | [Libraries](https://www.lua.org/manual/5.4/manual.html#6) |
| PHP | Common core and extension functions | `BcMath`, `Dom`, and `Random` use `stdlib` | [Function reference](https://www.php.net/manual/en/funcref.php) |
| Python | Builtin functions, types, and exceptions | `builtins`, `sys`, and `time` use runtime precedence; filesystem libraries use project precedence | [Builtins](https://docs.python.org/3/library/functions.html), [library index](https://docs.python.org/3/library/index.html) |
| Ruby | Implicit Kernel provider | Bundled roots plus pp symbol provider | [Kernel](https://docs.ruby-lang.org/en/master/Kernel.html) |
| Rust | Prelude names; Rc and Arc require imports | mem, cmp, and iter symbol providers under std/core roots | [Prelude](https://doc.rust-lang.org/std/prelude/index.html) |
| Scala | Implicit Predef provider | Explicit math provider; scala/java module policy | [Predef](https://www.scala-lang.org/api/current/scala/Predef$.html) |
| Swift | Implicit Swift provider | Foundation symbols; platform framework roots | [Standard library](https://developer.apple.com/documentation/swift) |
| TypeScript / JavaScript | ECMAScript constructors/global functions and common Node globals | Node module roots use runtime precedence, including explicit `node:` spellings | [Node modules](https://nodejs.org/api/modules.html#built-in-modules) |
| Zig | Reserved `@` builtin functions | `std` and `builtin` are external | [Builtins](https://ziglang.org/documentation/master/#Builtin-Functions) |

Qualified library methods are not flattened into global builtin names. For
example, `std::move`, `Math.max`, and `Console.WriteLine` follow their owner or
import identity. Language keywords remain the responsibility of syntax rules.

## Limits

The lists are not exhaustive across runtime versions, platforms, optional
extensions, and user modifications to module loaders. PHP extension APIs, Swift
frameworks, and Zig builtins vary with the target runtime. Runtime module-cache
mutation is not modeled.

The original `code-graph` crate does not contain an equivalent complete registry.
Its generic language implementations include primitive-type exclusions, which are
not interchangeable with callable builtin names. Its Rust custom pipeline also
distinguishes `std`, `core`, and `alloc` from other external crates.

Tests cover Node builtin versus relative imports through snapshot edits, Python
project modules shadowing standard modules, and package siblings that must not
capture absolute imports. C/C++ tests retain project functions sharing standard
function names. Existing language fixtures remain enabled.
