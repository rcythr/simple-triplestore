<pre>
              ██████╗  ██████╗██╗   ██╗████████╗██╗  ██╗██████╗  █╗ ███████╗
              ██╔══██╗██╔════╝╚██╗ ██╔╝╚══██╔══╝██║  ██║██╔══██╗ ╚╝ ██╔════╝
              ██████╔╝██║      ╚████╔╝    ██║   ███████║██████╔╝    ███████╗
              ██╔══██╗██║       ╚██╔╝     ██║   ██╔══██║██╔══██╗    ╚════██║
              ██║  ██║╚██████╗   ██║      ██║   ██║  ██║██║  ██║    ███████║
              ╚═╝  ╚═╝ ╚═════╝   ╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝    ╚══════╝

                            ____ _ _  _ ___  _    ____
                            [__  | |\/| |__] |    |___
                            ___] | |  | |    |___ |___

 
    ████████╗██████╗ ██╗██████╗ ██╗     ███████╗███████╗████████╗ ██████╗ ██████╗ ███████╗
    ╚══██╔══╝██╔══██╗██║██╔══██╗██║     ██╔════╝██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██╔════╝
       ██║   ██████╔╝██║██████╔╝██║     █████╗  ███████╗   ██║   ██║   ██║██████╔╝█████╗  
       ██║   ██╔══██╗██║██╔═══╝ ██║     ██╔══╝  ╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  
       ██║   ██║  ██║██║██║     ███████╗███████╗███████║   ██║   ╚██████╔╝██║  ██║███████╗
       ╚═╝   ╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝╚══════╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝
</pre>

A [triplestore](https://en.wikipedia.org/wiki/Triplestore) which can be used as a flexible graph database with support for custom node and edge properties.

## Data Model
Each node and edge is assigned an [Ulid][ulid::Ulid]. Property data is then associated with this id using key-value storage.

Graph relationships are stored three times as `(Ulid, Ulid, Ulid) -> Ulid` with the following key orders:
  * Subject, Predicate, Object
  * Predicate, Object, Subject
  * Object, Subject, Predicate

This allows for any graph query to be decomposed into a range query on the lookup with the ideal ordering.

## Query

## Supported Key-Value Backends
  * [Memory][MemTripleStore] ( std::collections::BTreeMap )
  * [Sled][SledTripleStore]
