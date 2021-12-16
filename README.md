## legs 🦵

Legs is an interface for [go-data-transfer](https://github.com/filecoin-project/go-data-transfer),
providing a 1:1 mechanism for maintaining a synchronized [IPLD dag](https://docs.ipld.io/) of data between
a publisher and a subscriber's current state for that publisher.

## Usage

Typically an application will be either a provider or a subscriber, but may be both.

### Publisher

Create a legs publisher.  Update its root to cause it to publish.

```golang
pub, err :=  NewPublisher(host, dsstore, lsys, "/legs/topic")
if err != nil {
	panic(err)
}
...
// Publish updated root.
err = publisher.UpdateRoot(ctx, lnk.(cidlink.Link).Cid)
if err != nil {
	panic(err)
}
```

### Subscriber

The `Broker` handles subscribing to a topic, reading messages from the topic and routing the messages to the handler that is responsible for keeping track of a publisher.

Create a `Broker`:

```golang
bkr, err := legs.NewBroker(dstHost, dstStore, dstLnkS, "/legs/topic", nil)
if err != nil {
	panic(err)
}

```
Optionally, request notification of updates:

```golang
watcher, cancelWatcher := bkr.OnSyncFinished()
defer cancelWatcher()
go watch(watcher)

func watch(notifications <-chan legs.SyncFinished) {
    for {
        syncFinished := <-notifications
        // newHead is now available in the local dataStore
    }
}
```

To shutdown a `Broker`, call its `Close()` method.

Brokers can be created with a `AllowPeer` function.  This function determines if the broker handles messages from a publisher, when a message is received from a new publisher with whom a sync has not already been done.

Brokers keep track of the latest head for each publisher that it has synced to avoid exchanging the whole DAG from scratch in every update, instead downloading only the part that has not been synced. This value is not persisted as part
of the library. If you want to start a `Broker` which has already partially synced with a provider you can use:
```golang
bkr, err := legs.NewBroker(dstHost, dstStore, dstLnkS, "/legs/topic", allowPeer)
if err != nil {
    panic(err)
}
// Set up partially synced publishers
if err = brk.SetLatestSync(peerID1, lastSync1) ; err != nil {
    panic(err)
}
if err = brk.SetLatestSync(peerID2, lastSync2) ; err != nil {
    panic(err)
}
if err = brk.SetLatestSync(peerID3, lastSync3) ; err != nil {
    panic(err)
}
```

License
---

Legs is dual-licensed under Apache 2.0 and MIT terms:

    Apache License, Version 2.0, (LICENSE or http://www.apache.org/licenses/LICENSE-2.0)
    MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)
