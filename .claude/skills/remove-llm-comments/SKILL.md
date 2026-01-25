---
name: remove-llm-comments
description: Remove unnecessary LLM-generated comments from code
allowed-tools: Read, Edit, Glob, Grep
---

# Remove LLM comments

LLMs leave behind comments that no human would write. They clutter the code and make it obvious an AI wrote it.

## What to remove

### Obvious narration

```rust
// Create a new vector to store results
let results = Vec::new();

// Iterate over items
for item in items {
```

The code already says what it does. Delete these.

### Changelog-style comments

```python
# Added error handling for edge cases
# Updated to use new API
# Fixed bug where X would fail
```

That's what git history is for.

### Section markers that add nothing

```go
// ============================================
// HELPER FUNCTIONS
// ============================================
```

If you need these, the file is too long. Split it.

### Filler explanations

```typescript
// This function takes a user ID and returns the user object
function getUser(userId: string): User {
```

The signature already tells you this.

## What to keep

- Comments explaining *why*, not what (business logic, non-obvious constraints)
- Links to issues, specs, or external docs
- Warnings about gotchas or edge cases
- License headers
- Doc comments for public APIs

## Example

Before:
```rust
// Import the necessary modules for HTTP handling
use axum::{Router, routing::get};

// Create the main application router
// This router will handle all incoming HTTP requests
fn create_router() -> Router {
    // Build the router with a health check endpoint
    Router::new()
        // Add a GET route for the health check
        .route("/health", get(health_check))
}

// Health check handler function
// Returns a simple OK response to indicate the service is running
async fn health_check() -> &'static str {
    "ok"
}
```

After:
```rust
use axum::{Router, routing::get};

fn create_router() -> Router {
    Router::new()
        .route("/health", get(health_check))
}

async fn health_check() -> &'static str {
    "ok"
}
```
