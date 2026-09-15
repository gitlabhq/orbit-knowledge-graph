# Scaling Code Graph Indexing to support Commits/Branches

This document supersedes the previous design document about [code indexing](./code_indexing.md), specifically, code parsing and resolution.

## Background

Code graph indexing is the process of filtering and parsing source code to build a graph of relationships (calls, imports, etc.) between common code constructs like Definitions (functions, classes, etc.). This also may include including import metadata to determine if a definition is internal or external to the repository.

Currently, code graphs are constructed only for the default branch of a respository. This will soon no longer be be the case. Thus, we must consider what changes are needed to support indexing code for branches and commits.

It is naively reasonable to assume that we don't need to do much. Techncially, all that's required for a code graph to be indexed is a stream of files and some basic git metadata, like what revision those files belong to. 

Now that Code Indexing v2 will be maintaining a "lightweight clone" of Gitaly data, corresponding to a full base commit + N diffs (one per branch tip), it is again reasonable to assume that we can just do N+1 runs of the code indexer during a backfill by just fetching all the (base, diff) pairs from object storage.

## Problem

[TODO]



