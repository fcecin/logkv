# logkv

__NOTE: This is experimental software.__

A key-value store for C++20 inspired by [Prevayler](https://prevayler.org/).

It persists any associative container by logging its entry updates and saving snapshots of its state to a backing data directory. If the application crashes, state can be restored by loading the latest snapshot and replaying the latest entry update events since that snapshot.

Motivation:

* Takes care of events; just needs serialization code for keys and values.
* Optional logging of entry updates; can just modify the backing container object directly.
* No data indexing or compaction concerns due to snapshotting.
