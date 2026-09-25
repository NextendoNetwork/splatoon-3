package main

import "testing"

func TestPoolBroadcastReachesEverySubscriberInThatPool(t *testing.T) {
	const pool = "zara"
	a := enregistrerAbonnementLobby("u-pool-a", pool)
	b := enregistrerAbonnementLobby("u-pool-b", pool)
	c := enregistrerAbonnementLobby("u-pool-c", "another-pool")
	defer oublierCanalDeHall("u-pool-a")
	defer oublierCanalDeHall("u-pool-b")
	defer oublierCanalDeHall("u-pool-c")

	message := []byte("room notification")
	if got := distribuerDansLobby(pool, message); got != 2 {
		t.Fatalf("delivered to %d subscribers, want 2", got)
	}
	for name, ch := range map[string]<-chan []byte{"a": a, "b": b} {
		select {
		case got := <-ch:
			if string(got) != string(message) {
				t.Errorf("subscriber %s received %q", name, got)
			}
		default:
			t.Errorf("subscriber %s did not receive pool notification", name)
		}
	}
	select {
	case got := <-c:
		t.Fatalf("different pool received %q", got)
	default:
	}
}
