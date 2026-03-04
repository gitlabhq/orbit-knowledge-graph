# sdlc_v2 with cursor based pagination

The following document describes the implementaiton path for a second non-backward compatible version of the sdlc module in the indexer. The reason for this change is to pay up technical debt and build a more extensible design that allow more flexibility in SQL query building while offering best in class resilience to failure. Please be careful about storing old code snippet in the context window which could negatively impact the code output of the second version. 

## Architecture overview

Ontology -> IR -> Extract SQL for Batch -> DataFusion -> Nodes and Edges -> Load in ClickHouse -> Updates IR with Cursor -> Extract SQL for next Batch -> ... -> Done.

### Topics

The system will still listen to two topics, one for global and one for namespace, but will converge to the same code. This means that there will be two handlers, but as stated they will converge to the same function which will perform the extract, transform and load. They are currently different, but this does not make any sense, both of them are abstract using Arrow and DataFusion to extract node and edges and that's something we want to continue leveraging for this new design.

### SQL Building

The system will be based on the Ontology, which can be modified to fit our new needs, and will be used as a base to build a IR that can be used to generate the extract SQL and the Nodes and Edges data fusion transform. Ideally it is immutable and can be update via something like `cursor = cursor.advance(...)`. The extract SQL can be re-built after each batch, that is ok.

### Extract 

The extraction is still done using the ClickHouse arrow client and fed into different sessions of DataFusion to avoid memory overhead. The default batch size is of 1 million.

### Transform

The transformation is still done using DataFusion query and derived from the Ontology.

### Load

Writing to ClickHouse is still done using Arrow and in batch.

### Watermark + Cursor

The system is still built on watermarks, but with a caveat: it now has a cursor that is based on the ordering keys (see ~/Desktop/default-value-reconciler.md). The first page has no cursor but should still be updated according to the order by keys and the subsequent page should use the last result received to start the next batch. Everytime a batch is successfully written we update the cursor in the database. Ideally both global and namespaced entity share the same table. We could honestly even consider using nats for that part to remove code (and tbh I like that more than using ClickHouse for it). 

If a query fail, we obviously retry 3 time with backoff and if it still fail we fail (the job will be retried later). 

#### Retries

On next run, the system will load the last known watermark. If that watermark still has a cursor, it means the last run has not completed and we must restart there. If the cursor is empty we perform an incremental diff.

## Non-functional goals.

- The code must read like a story, where we are able to follow it from top to bottom.
- The code must only have comments where relevant (ex: does not restate the function name).
- Follow good coding principles, TDA, YAGNI, KISS, black box testing (test the entry function has a blackbox) and avoid acronyms when possible.
- The resulting code must be in a separate sdlc_v2 module and have 0 dependencies on sdlc.

### Metrics

The code should re-use the same metrics (can create a duplicate file for encapsulation)

## Links

Cursor based pagniation specification: ~/Desktop/default-value-reconciler.md

