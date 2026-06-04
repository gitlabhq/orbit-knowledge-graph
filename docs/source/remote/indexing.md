---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: What data Orbit indexes, which languages are supported for code indexing, and how indexing is scoped.
title: What Orbit indexes
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

## Scope

Orbit indexes top-level groups only. Enable Orbit on a top-level group and all its
subgroups and projects are indexed automatically. You cannot enable Orbit on a
subgroup or individual project.

## SDLC data

Orbit indexes the following GitLab objects and their relationships:

| Domain | Objects indexed |
|--------|----------------|
| Core | Groups, projects, users, notes (comments) |
| Code review | Merge requests, merge request diffs, changed files |
| CI/CD | Pipelines, stages, jobs |
| Planning | Work items (issues, epics, tasks, incidents), milestones, labels |
| Security | Vulnerabilities, security findings, security scans, scanners, CVE/CWE identifiers |

SDLC data is updated continuously via change data capture. Changes in your GitLab instance
appear in Orbit within minutes.

## Source code

Orbit indexes source code from your repositories and builds a code graph on top of it.

What gets indexed:

- Files and directories
- Function, class, and module definitions (with start/end line and full source content)
- Import and cross-file reference relationships between files

Code is indexed from the default branch only. Orbit re-indexes automatically when
the default branch changes.

### Supported languages

| Language | File extensions | Definitions | Cross-file references | Framework / ecosystem support |
|----------|----------------|-------------|----------------------|-------------------------------|
| Ruby | `.rb`, `.rbw`, `.rake`, `.gemspec` | Yes | Yes | Rails, Forwardable |
| Java | `.java` | Yes | Yes | Annotations, records |
| Kotlin | `.kt`, `.kts` | Yes | Yes | Extension functions, operator desugaring |
| Python | `.py` | Yes | Yes | Relative imports, decorators |
| TypeScript | `.ts`, `.tsx`, `.mts`, `.cts` | Yes | Yes | Path aliases (`tsconfig.json`) |
| JavaScript | `.js`, `.jsx`, `.mjs`, `.cjs`, `.vue`, `.graphql`, `.gql`, `.json` | Yes | Yes | Vue SFC, React/JSX, CommonJS, ESM |
| Rust | `.rs` | Yes | Yes | Cargo workspaces, macro expansion |
| Go | `.go` | Yes | Yes | Struct embedding, composite literals |
| C# | `.cs` | Yes | Yes | Records, `using static`, attributes |
| C | `.c`, `.h` | Yes | Yes | Include graph |
| C++ | `.cpp`, `.cc`, `.cxx`, `.hpp`, `.hh`, `.hxx` | Yes | Yes | Namespaces, include graph |

Languages not currently indexed: Swift, COBOL, Terraform, YAML.

### Language details

Each language section below describes what the code indexer extracts and resolves.
All languages extract function, class, and module definitions with start/end lines
and full source content. The differences are in what additional constructs, frameworks,
and resolution strategies each language supports.

#### Ruby

Definitions extracted: classes, modules, methods, singleton methods, singleton classes,
constants, and lambdas. Class inheritance and module mixins (`include`, `extend`, `prepend`)
are tracked as super-type relationships.

Rails and Ruby DSL support:

- `attr_accessor`, `attr_reader`, `attr_writer` emit attribute definitions
- `class_attribute`, `mattr_accessor`, `cattr_accessor` (and writer/reader variants) emit attribute definitions
- `has_many`, `belongs_to`, `has_one`, `has_and_belongs_to_many` emit method definitions
- `scope` emits static method definitions
- `delegate` and Forwardable's `def_delegators`/`def_delegator` emit method definitions
- `define_method` emits method definitions from symbol or string arguments
- `alias` emits method definitions
- `send`, `public_send`, `__send__` are rewritten to resolve the symbol argument as a method name

Cross-file resolution uses scope-based FQN walking, same-file resolution, and global name
matching. Constructor chains (`Model.new.save!`) resolve through `new`, `find`, `find_by`,
`create`, `first`, and `last`.

Not tracked: `method_missing`, `respond_to_missing?`, `class_eval`, `instance_eval`.

#### Java

Definitions extracted: classes, enums, records, interfaces, annotation declarations,
enum constants, fields (with type), constructors, methods (with return type), and lambdas.
Inheritance and interface implementation are tracked as super-type relationships.

Type annotations on bindings (variables, parameters, fields) are used during resolution.
Annotations on definitions are extracted. Method references (`Class::method`) and
`new` constructor expressions are resolved.

Cross-file resolution uses explicit imports, wildcard imports (`import java.util.*`),
static imports, same-package matching, and same-file matching.

Java and Kotlin share a JVM language family, so definitions from `.java` and `.kt` files
in the same project are resolved against each other.

Not tracked: annotation processor output, generic type parameters, anonymous inner classes.

#### Kotlin

Definitions extracted: classes, data classes, value classes, annotation classes, enums,
interfaces, objects, companion objects, functions, extension functions (with receiver type),
constructors, properties, extension properties (with receiver type), enum entries, and lambdas.

Operator expressions (`+`, `-`, `*`, `/`) are desugared to their corresponding method names
(`plus`, `minus`, `times`, `div`) and resolved as calls on the receiver.

Extension functions and properties carry their receiver type in metadata. Companion object
members are accessible through implicit sub-scopes.

Cross-file resolution uses the same strategies as Java. Kotlin shares the JVM language family
with Java for bidirectional package-based resolution.

Not tracked: coroutine-specific analysis, delegation via `by`, sealed class hierarchies.

#### Python

Definitions extracted: classes (with inheritance), functions, async functions, methods,
async methods, decorated variants of all the above, and lambdas. Decorators are extracted
and stored as metadata. Return type annotations are captured.

Module scope is derived from the file path (`services/user_service.py` becomes
`services.user_service`; `__init__.py` maps to the parent module name).

Import resolution handles `import x`, `from x import y`, relative imports
(`from .models import User` with dot-counting), wildcard imports, and aliased imports.

Cross-file resolution uses explicit imports, wildcard imports, file-path-based module
matching, scope-based FQN walking, and same-file matching.

Not tracked: dynamic attribute access (`getattr`), `*args`/`**kwargs` flow,
comprehension variable scoping.

#### TypeScript

Definitions extracted: classes (with `extends`), functions, methods (instance and static),
interfaces, type aliases, enums, enum members, namespaces, and variables. Type annotations
on definitions are captured.

Uses the OXC parser. Module resolution probes extensions in TypeScript-first order
(`ts`, `tsx`, `js`, `jsx`, `mjs`, `cjs`, `mts`, `cts`, `vue`, `graphql`, `gql`, `json`)
and supports `index.{ext}` directory imports.

Path alias resolution reads `tsconfig.json` and `jsconfig.json`.

Cross-file resolution builds a full module graph with ESM and CommonJS support.
Star re-export chains are resolved transitively (capped at 64 per file).

#### JavaScript

Shares the same OXC-based pipeline as TypeScript. All TypeScript features apply, plus:

- **Vue SFC**: `.vue` files have their `<script>` and `<script setup>` blocks extracted
  and analyzed. Both Options API (`methods`, `computed`, `watch`, `data`, lifecycle hooks)
  and `<script setup>` patterns are supported. Component detection recognizes
  `defineComponent`, `defineAsyncComponent`, `defineNuxtComponent`, and `Vue.extend`.
- **React/JSX**: JSX element invocations (`<Component />`, `<Ns.Component />`) are tracked
  as call edges. Intrinsic elements (lowercase tags like `<div>`) are filtered out.
- **CommonJS**: `require()` with destructuring, `module.exports`, and `exports.foo`
  assignments are resolved alongside ESM imports/exports.
- **GraphQL**: `.graphql` and `.gql` files are treated as file-backed modules.
- **JSON**: `.json` files are treated as modules with a default export.
- **Webpack**: `webpack.config.{js,cjs,mjs,ts}` is read for module resolution aliases.
- **Bun**: detected via `bun.lock`, `bun.lockb`, or `bunfig.toml`; adjusts extension
  probe order.

Minified files (`.min.js`) are excluded.

Not tracked: `.svelte` and `.astro` files, dynamic `import()` expressions, decorator
metadata, Flow types.

#### Rust

Uses rust-analyzer for semantic analysis instead of tree-sitter.

Definitions extracted: modules, structs, enums, enum variants, traits, unions, functions,
methods, associated functions, constants, statics, type aliases, macros
(`macro_rules!` and `macro` definitions), and fields (including tuple struct fields by index).

Cargo workspace support: loads `Cargo.toml`, `Cargo.lock`, and workspace manifests to build
a crate graph. Falls back to standalone single-file analysis when workspace loading fails.

Macro expansion: declarative macros are expanded up to depth 8 (capped at 20,000 AST nodes).
Definitions and calls inside expanded macros are extracted.

Trait relationships: `trait Foo: Bar + Baz` emits extends edges. `impl Trait for Type` emits
extends edges from the type to the trait. When multiple `impl` blocks produce methods with
the same name (for example, `Display::fmt` and `Debug::fmt`), the trait name is inserted into
the FQN for disambiguation.

Cross-file resolution is semantic: every call, operator, macro invocation, `?` expression,
and `.await` is resolved to its definition site via rust-analyzer's HIR. An SSA-based local
flow index resolves calls through local variable assignments. Unresolved calls fall back to
matching against imported symbol names (excluding `std`, `core`, `alloc`).

Items gated by inactive `#[cfg(...)]` attributes are skipped.

Not tracked: proc-macro expansion, lifetime tracking, closure/fn-pointer receivers.

#### Go

Definitions extracted: functions (with return type), methods (with return type and receiver
type), interface method specs, structs, interfaces, and type declarations. Struct embedding
(unnamed fields) is tracked as a super-type relationship.

Test files (`_test.go`) are excluded.

Type annotations on bindings are used during resolution, including composite literal
constructors (`MyStruct{field: value}`) and pointer type dereferencing.

Import resolution handles standard imports, aliased imports, blank imports (`_`), and
dot imports (`.`).

Cross-file resolution uses explicit imports, scope-based FQN walking, and same-file matching.

Not tracked: goroutine/channel flow, interface satisfaction, `go generate`.

#### C\#

Definitions extracted: namespaces, classes, structs, records, enums, interfaces, methods
(with return type), constructors, properties (with type), fields (with type), events, enum
members, and lambdas. Inheritance and interface implementation are tracked.

`using` directives are resolved: `using System;` as a namespace wildcard import,
`using static System.Math;` as a static import, and `using Alias = Target;` as an aliased
import.

Attributes (`[Serializable]`) are extracted as references.

Cross-file resolution uses explicit imports, wildcard imports, scope-based FQN walking,
and same-file matching. `base` is resolved as the super-type reference.

Not tracked: LINQ expressions, partial class merging, `dynamic` type flow.

#### C

Definitions extracted: functions, structs, enums, unions, typedefs, and enum constants.

`#include` directives (both `"header.h"` and `<header.h>`) build an include graph for
cross-file resolution. Scope-based FQN walking and same-file matching are also used.

C and C++ share a language family, so definitions from `.c`, `.h`, `.cpp`, and related
files are resolved against each other.

Not tracked: macro expansion, function pointers, `#define` constants.

#### C++

Includes everything C supports, plus: namespaces, `class` definitions, range-based `for`
loops, `this` keyword resolution, and qualified identifier calls (`Ns::func()`).

C++ shares the C language family. `.h` headers are resolved against both C and C++ sources.

Not tracked: template specialization, operator overload resolution, virtual dispatch.

## What is not indexed

- Branches other than the default branch
- Binary files
- Files in archived projects (SDLC metadata for archived projects is still indexed)
- Private content the requesting user does not have access to (authorization is enforced at query time)

## Authorization

Orbit enforces GitLab access controls at query time. A query returns only entities
the requesting user has access to in GitLab. There is no separate Orbit permission model.

A group Owner who enables Orbit does not grant other users broader access than they
already have in GitLab.
