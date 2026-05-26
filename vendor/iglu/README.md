# Iglu Schema Repository

This is the Iglu Schema Repository for GitLab Snowplow served on https://gitlab-org.gitlab.io/iglu

## Prerequisites

Make sure you have the following things installed and running on your machine:

- Check [**Igluctl**](https://docs.snowplow.io/docs/api-reference/iglu/) on how to install it locally. Make sure you have `igluctl` available in your `PATH`.
- Read up on [JSON schema](http://json-schema.org/). Explore the [schemas](./public/schemas) already defined in this repository.

## Why do we need a Iglu Schema Repository

Snowplow supports a large number of events “out of the box” (first class citizens), many of which are common in a web or mobile analytics context. Examples of events that we support include:

- Page views
- Page pings
- Link clicks
- Form fill-ins (for the web)
- Form submissions
- Transactions

They also allow their users to create custom structured events which give us the possibility to set up to 5 pre-defined parameters:

- Category
- Action
- Label
- Property
- Value

If this is still not enough, and if you want to send more complex/comprehensive data, Snowplow gives you two options:

- **self-describing events**: these events are entirely customisable and look like the example below.
- **custom context**: custom contexts are additional information sent alongside any kind of event mentioned above. They are particularly useful to enrich a standard event with some additional data.

These two types of additional tracking have the same format: a [self-describing JSON](https://snowplow.io/blog/introducing-self-describing-jsons/).

_Snowplow Definition of self-describing JSON_

> Self-describing JSON is a standardised JSON format which co-locates a reference to the instance's JSON Schema alongside the instance's data.

Example of self-describing JSON:

```json
{
  "schema": "example.gitlab/event_name/jsonschema/1-0-0",
  "data": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

Each self-describing event or custom context needs to have **its dedicated JSON schema** (the value of the "schema" key). The JSON schema is designed to describe which data this specific event is expected to send. Here is an example of what a JSON schema looks like.

[Example of a JSON schema for a sendgrid self-describing events](https://raw.githubusercontent.com/snowplow/iglu-central/master/schemas/com.sendgrid/bounce/jsonschema/2-0-0)

Any time we send a self-describing event or a custom context, we will automatically validate it against the schema it claims to adhere to. If we see that a parameter is wrong, or that the value has an incorrect data type, we will be warned and the event will be filtered out of our data pipeline.

We store these JSON schema in this Iglu repo.

## Adding New Schema

### Schema naming conventions

For an event name `EVENT_NAME`, name your schema as:

- `EVENT_NAME` for self describing event schema
- `EVENT_NAME_context` for additional context for self describing event schema
- `pageview_context` for the standard pageview Snowplow event

- Create the new folder for `public/schemas/com.gitlab/EVENT_NAME/jsonschema/`
- Upload it as a text file named `1-0-0` for new schema.
- Follow semantic versioning. For example, for breaking updates, use `2-0-0`.

### If you create a new self-describing event or context

1. Create a new branch
1. Create a file in the repo for the new schema. The file path should like this:
   `public/schemas/com.gitlab/%NEW_CONTEXT_OR_EVENT%/jsonschema/1-0-0`
1. Define the schema according to the JSON schema guidelines.
1. Validate it using `igluctl` running the following command:

   ```bash
   igluctl lint public/schemas/com.%COMPANY%/%NEW_CONTEXT_OR_EVENT%
   ```

### Schema properties

#### GitLab Iglu JSON schema

Schema should conform to the 1-0-0 self-describing schema as described in this [link](http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#).
As described in this schema, the `vendor`, `name`, `format`, `version` are required:

```json
  "self": {
    "vendor": "com.gitlab",
    "name": "pageview_context",
    "version": "1-0-0",
    "format": "jsonschema"
  }
```

- `vendor` must be `com.gitlab`
- `name` must be given as described above in schema naming conventions
- `version` must start from 1-0-0 and must conform to semantic versioning.
- `format` must be `jsonschema`

Properties must have data types, formats, patterns and restrictions as described by json schema standard: http://json-schema.org/draft-04/schema

See an example schema here https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/gitlab_standard/jsonschema/1-0-0

### Update an existing self-describing event or context

To update existing context:

1. Create a file in the repo to increase the schema version. Use the following file path format and semantic versioning: `public/schemas/com.gitlab/%CONTEXT_OR_EVENT%/jsonschema/1-0-1`.
1. Update references to the new schema version. For example, the [gitlab_standard](https://docs.gitlab.com/ee/development/internal_analytics/snowplow/schemas.html#gitlab_standard) schema on the GitLab project at https://gitlab.com/gitlab-org/gitlab/-/blob/master/lib/gitlab/tracking/standard_context.rb#L6.

## Building

This is built and deployed by GitLab CI for GitLab Pages and deployed using CD to https://gitlab-org.gitlab.io/iglu

## Development

The Iglu Schema Repository is served under https://gitlab-org.gitlab.io/iglu.

Use the schema URI as `iglu:com.gitlab/EVENT_NAME/jsonschema/1-0-0`

Example for self describing events:

```ruby
Gitlab::Tracking.self_describing_event('iglu:com.gitlab/EVENT_NAME/jsonschema/1-0-0', { foo: 'bar' })
```

Incorrect Schema URIs:

- `iglu:io.gitlab.gitlab-org/iglu/schemas/com.gitlab/EVENT_NAME/jsonschema/1-0-0`
- `https://:gitlab-org.gitlab.io/iglu/schemas/com.gitlab/EVENT_NAME/jsonschema/1-0-0`

## Additional resources

Documentation on JSON Schema:

- Other example JSON Schema can be found in [Iglu Central](https://github.com/snowplow/iglu-central/tree/master/schemas). Note how schemas are namespaced in different folders.
- [Schema Guru command line tool](https://github.com/snowplow/schema-guru) for programmatically generating schemas from existing JSON data.
- [Snowplow 0.9.5 release blog post](https://snowplow.io/blog/snowplow-0.9.5-released-with-json-validation-shredding/), which gives an overview of the way that Snowplow uses JSON Schema to process, validate and shred unstructured event and custom context JSONs.
- It can be useful to test JSON Schema using online validators e.g. [this one](https://jsonschemalint.com)
- [json-schema.org](http://json-schema.org/) contains links to the actual JSON Schema specification, examples and guide for schema authors.
- The original specification for self-describing JSONs, produced by the Snowplow team, can be found [here](https://snowplow.io/blog/2014/05/15/introducing-self-describing-jsons/).

Documentation on JSONPath:

- Example JSONPath files can be found on the [Snowplow repo](https://github.com/snowplow/snowplow/tree/master/4-storage/redshift-storage/jsonpaths). Note that the corresponding JSON Schema definitions are stored in [Iglu central](https://github.com/snowplow/iglu-central/tree/master/schemas).
- Amazon documentation on JSONPath files can be found [here](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html).

Documentation on creating tables in Redshift:

- Example Redshift table definitions can be found on the [Snowplow repo](https://github.com/snowplow/snowplow/tree/master/4-storage/redshift-storage/sql). Note that corresponding jsonschema definitions are stored in [Iglu central](https://github.com/snowplow/iglu-central/tree/master/schemas).
