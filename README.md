legs 🦵
===

Legs is a simplified interface for [go-data-transfer](https://github.com/filecoin-project/go-data-transfer),
providing a 1:1 mechanism for maintaining a synchronized [IPLD dag](https://docs.ipld.io/) of data between
a publisher and subscriber.

Usage
---

Creating a legs publisher is as simple as:

```golang
publisher, err := legs.Publish(ctx, dataStore, lp2pHost, "legs/topic")
...
if err := publisher.UpdateRoot(ctx, lnk.(cidlink.Link).Cid); err != nil {
	panic(err)
}
```

A subscriber to the same "legs/topic" would watch via

```golang
subscriber, err := legs.Subscribe(ctx, dataStore, lp2pHost, "legs/topic")
subscriptionWatcher, cncl := subscriber.OnChange()
go watch(subscriptionWatcher)

func watch(notifications chan cid.Cid) {
    for {
        newHead := <-notifications
        // newHead is now available in the local dataStore
    }
}
```

License
---

Legs is dual-licensed under Apache 2.0 and MIT terms:

    Apache License, Version 2.0, (LICENSE or http://www.apache.org/licenses/LICENSE-2.0)
    MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)
