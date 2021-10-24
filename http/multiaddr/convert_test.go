package multiaddr

import (
	"net/url"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	samples := []string{
		"http://www.google.com/path/to/rsrc",
		"https://protocol.ai",
		"http://192.168.0.1:8080/admin",
		"https://[2a00:1450:400e:80d::200e]:443/",
		"https://[2a00:1450:400e:80d::200e]/",
	}

	for _, s := range samples {
		u, _ := url.Parse(s)
		mu, err := ToMA(u)
		if err != nil {
			t.Fatal(err)
		}
		u2, err := ToURL(*mu)
		if u2.Scheme != u.Scheme {
			t.Fatalf("scheme didn't roundtrip. got %s expected %s", u2.Scheme, u.Scheme)
		}
		if u2.Host != u.Host {
			t.Fatalf("host didn't roundtrip. got %s, expected %s", u2.Host, u.Host)
		}
		if u2.Path != u.Path {
			t.Fatalf("path didn't roundtrip. got %s, expected %s", u2.Path, u.Path)
		}
	}
}
