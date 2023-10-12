# OpenSearch Loader
This is the user documentation for the OpenSearch data loader module contained in the ICDC-Dataloader utility.

**NOTE: This document is in progress and is currently incomplete**

## Introduction
The OpenSearch data loader module creates an initializes OpenSearch indexes using an index definition file as a specification and a Neo4j database as a data source.

## Cypher Queries for Loading
The return values of cypher query used to query Neo4j must match one of the two below formats.

### Format 1: Match Index Structure
Each row of the query return contains properties that match the OpenSearch index property names

#### Example:
```
MATCH (n:node)
RETURN
    n.prop1 AS indexProperty1,
    n.prop2 AS indexProperty2
    ...
```

### Format 2: Wrap Index Properties in an Object
Each row of the query return contains a single object called **"opensearch_data"**. This object will contain properties that match the OpenSearch index property names.

#### Example:
```
MATCH (n:node)
WITH {
    indexProperty1: n.prop1,
    indexProperty2: n.prop2
    ...
} AS opensearch_data
RETURN opensearch_data
```
