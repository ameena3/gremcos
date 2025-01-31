# gremcos

Gremcos is an Golang driver for [Gremlin API of Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/graph-introduction) which is a Graph Database (Gremlin API) for Azure.

Gremcos is a fast, efficient, and easy-to-use client for the [TinkerPop](http://tinkerpop.apache.org/docs/current/reference/) graph database stack. It is a gremlin language driver which uses WebSockets to interface with [gremlin server](http://tinkerpop.apache.org/docs/current/reference/#gremlin-server) and has a strong emphasis on concurrency and scalability. Please keep in mind that gremcos is still under heavy development and although effort is being made to fully cover gremcos with reliable tests, bugs may be present in several areas.

## Installation

```bash
go get github.com/ameena3/gremcos
```

## Examples

- See: [examples/README.md](examples/README.md)

## Hints

### Response Format

This implementation supports [Graphson 2.0](http://tinkerpop.apache.org/docs/3.4.0/dev/io/#graphson-2d0) (not 3) in order to be compatible to CosmosDB. This means all the responses from the CosmosDB server as well as the responses from the local gremlin-server have to comply with the 2.0 format.

### Azure Cosmos Gremlin Implementation Differences

This implementation is only working/ compatible with [TinkerPop 3.4.0](http://tinkerpop.apache.org/downloads.html).

Cosmos DB specific error handling is done and described at [ErrorHandling.md](ErrorHandling.md). For example error responses returned by Cosmos due to a usage rate limit violation are handled accordingly.

### Local Development

For being able to develop locally against a local graph data base one can start a local gremlin-server via `make infra.up`.
In order to be able to use all features the query language has to be switched to `QueryLanguageTinkerpopGremlin`.

### Switch the Query Language

Since the query language of the Cosmos DB and the tinkerpop gremlin implementation are not 100% compatible it is possible to set the language based on the use-case.
The following piece of code depicts how to set the query language.

```go
    // If you want to run your queries against a apache tinkerpop gremlin server it is recommended
    // to switch the used query language to QueryLanguageTinkerpopGremlin.
    // Per default the CosmosDB compatible query language will be used.
    api.SetQueryLanguageTo(api.QueryLanguageTinkerpopGremlin)
```

## License

See [LICENSE](LICENSE.md)

### 3rd Party Licenses

- [difflib license](https://github.com/pmezard/go-difflib/blob/master/LICENSE)
