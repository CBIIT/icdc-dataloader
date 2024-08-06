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
## Pagination
For large amounts of data, sending multiple paginated index queries may be necessary to avoid exceeding Neo4j memory limits. In some cases, pagination might also improve loading performance. For each index loading query in the "cypher_queries" lists, the following conditions must be met for pagination to be enabled:
1. The **page_size** property in the "cypher_queries" entry must be present and set to an integer greater than 1
2. The **query** property in the "cypher_queries" entry must be a cypher query that includes the pagination variables **$skip** and **$limit**.

If either of these conditions are not met, the OpenSearch loader will still run the loading query but pagination will be disabled.

### Adding Pagination to Index Queries
Pagination can be added to a query by inserting the following line after a line starting with the **WITH** keyword:
```
SKIP $skip LIMIT $limit
```
This pagination line should be added to the query as early as possible, after the primary node type of the index is added to the graph. If the pagination line is added at the end, then it will have no impact on Neo4j memory efficiency or execution speed.
#### Examples:
In the following examples the primary node type of the index is **primary_node**
```
MATCH (x:primary_node)
WITH DISTINCT x
SKIP $skip LIMIT $limit
OPTIONAL MATCH (x)<--(a:node_a)
RETURN 
    x.name AS x_name,
    a.name AS a_name
```
In the following example the primary node type of the index is **primary_node**
```
MATCH (a:node_a)
WHERE a.color = "green"
MATCH (a)<--(x:primary_node)
WITH DISTINCT x, a
SKIP $skip LIMIT $limit
OPTIONAL MATCH (x)<--(b:b_node)
WITH a, x, COLLECT(DISTINCT b.name) AS list_of_b_names
RETURN 
    a.name AS a_name,
    x.name AS x_name,
    list_of_b_names AS b_names
```
