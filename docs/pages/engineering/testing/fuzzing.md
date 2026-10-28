---
title: Fuzzing
description: How Orbit sends random input to its parsers and decoders.
order: 7
authors:
  - aalgutifan
  - michaelusa
---

Fuzz targets send random input to the query languages, the source
parsers, and the message decoders. A short run happens on each merge request.
A longer run happens each day and grows the corpus.

## Contracts

- Each target keeps seed inputs in the repository and its grown corpus in
  private storage.
- The short run replays each corpus and then sends random input for a short
  time. It does not block a merge.
- The daily run uses an address sanitizer, runs in shards, and saves the
  grown corpus.
- Crashes go to private storage. Fuzzer output goes to a file, never to the
  public job log.
