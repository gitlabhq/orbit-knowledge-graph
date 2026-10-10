# Standard names in code indexing

Language lists live in `crates/code-graph-incremental/langs/*.yaml`.
They describe lookup policy, not a complete API inventory or a runtime emulator.

## Configuration

- `link.builtins`: callable names supplied by the language or runtime. Local
  bindings and concrete imported definitions still resolve. A matching name does
  not create a speculative call to an unresolved wildcard import.
- `resolve.external`: module roots that take precedence over repository lookup.
  Node's `fs` and `node:fs` belong here. Relative `./fs` is a different identity.
- `resolve.stdlib`: module names or provider entries with optional symbols and precedence.
  Plain module names use project precedence.
  Exact paths and declared source roots are checked first. Heuristic fallback
  roots and guessed submodules cannot capture these imports.

```yaml
config:
  resolve:
    stdlib:
      - module: stdio.h
        symbols: [printf, puts]
      - module: time
        precedence: runtime
      - json
```

`symbols` applies only to an import of that exact canonical module path. These
names suppress speculative wildcard-call fallback for that provider, while concrete
project definitions still resolve. A spelling such as `printf` is not implicitly
builtin merely because one standard header supplies it.

`precedence: runtime` gives the module the same lookup precedence as `external`.
Explicit project aliases are applied before either module classification. The
default, `precedence: project`, retains exact and declared-root project lookup.
Provider lists are compiled once when language rules load. They do not add a
new resolution pass or inspect language-specific syntax in the shared engine.

Both module lists match the first slash-separated component of the canonical
import path. They are not regexes or arbitrary string prefixes. Language YAML
normalizes namespace separators before this check. The original canonical path
is retained across resolution and snapshots, even when the resolved path changes.

Snapshot version 15 requires a fresh index for older snapshots, which lack that
original-path metadata.

## Per-language coverage

The YAML files hold the enumerated lists. This table explains their scope and
links to language references for maintaining them.

| Language | Builtin callable names | Module classification | Reference |
|---|---|---|---|
| Bash | Shell builtins such as `printf`, `read`, `cd`, and `declare` | No reserved source-file roots | [Bash builtins](https://www.gnu.org/software/bash/manual/html_node/Shell-Builtin-Commands.html) |
| C | No implicit library functions; symbols belong to header providers | Standard headers use `stdlib`; project headers remain eligible | [C library](https://en.cppreference.com/w/c/header.html) |
| C++ | No implicit library functions; symbols belong to header providers | Standard headers use `stdlib` | [C++ headers](https://en.cppreference.com/w/cpp/header.html) |
| C# | Empty: no implicit global standard functions | `System` uses `stdlib` | [Namespaces](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/namespace) |
| Elixir | Kernel functions and guards | Core module roots use `stdlib` | [Kernel](https://hexdocs.pm/elixir/Kernel.html) |
| Go | Predeclared functions and conversion types | Standard package roots use `external` | [Builtins](https://pkg.go.dev/builtin), [packages](https://pkg.go.dev/std) |
| Java | Implicit `java.lang` constructors | `java` is external; `javax` and `jdk` use `stdlib` | [java.lang](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/package-summary.html) |
| Kotlin | Default-imported functions, factories, and constructors | `kotlin` and `java` are external; `javax` and `jdk` use `stdlib` | [Default imports](https://kotlinlang.org/docs/packages.html#default-imports) |
| Lua | Global base-library functions | Normally preloaded modules use runtime precedence | [Libraries](https://www.lua.org/manual/5.4/manual.html#6) |
| PHP | Common core and extension functions | `BcMath`, `Dom`, and `Random` use `stdlib` | [Function reference](https://www.php.net/manual/en/funcref.php) |
| Python | Builtin functions, types, and exceptions | `builtins`, `sys`, and `time` use runtime precedence; filesystem libraries use project precedence | [Builtins](https://docs.python.org/3/library/functions.html), [library index](https://docs.python.org/3/library/index.html) |
| Ruby | Kernel functions | Bundled library roots use `stdlib` | [Kernel](https://docs.ruby-lang.org/en/master/Kernel.html) |
| Rust | Prelude constructors, functions, and existing builtin macro names | `std`, `core`, `alloc`, `proc_macro`, and `test` use `stdlib` | [Prelude](https://doc.rust-lang.org/std/prelude/index.html) |
| Scala | Predef functions | `java` is external; `scala`, `javax`, and `jdk` use `stdlib` | [Predef](https://www.scala-lang.org/api/current/scala/Predef$.html) |
| Swift | Global standard functions | `Swift` is external; platform frameworks use `stdlib` | [Standard library](https://developer.apple.com/documentation/swift) |
| TypeScript / JavaScript | ECMAScript constructors/global functions and common Node globals | Node module roots use `external`, including explicit `node:` spellings | [Node modules](https://nodejs.org/api/modules.html#built-in-modules) |
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
