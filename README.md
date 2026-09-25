<h1 align="center">splatoon-3</h1>

<p align="center">
  <b>Nextendo Network game server for Splatoon 3.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/license-PolyForm%20Shield%201.0.0-orange" alt="License: PolyForm Shield 1.0.0">
  <img src="https://img.shields.io/badge/go-1.26%2B-00ADD8" alt="Go 1.26+">
</p>

---

## What is this?

The game server for **Splatoon 3** on [Nextendo Network](https://nextendo.network).

Splatoon 3 does not use NEX. It talks **NPLN** — Nintendo's newer stack: gRPC over HTTP/2, with
its own authentication, a Firestore-shaped document/watch service (`Gamesync`), matchmaking,
friends and presence, cloud saves, schedules, and the Splatfest services. This repository is a
from-scratch implementation of the server side of that stack, sufficient to bring the game's
online mode up: booting into the square, friends and presence, private matches, public
matchmaking, end-of-match verdicts, and Splatfests.

It is a separate lineage from the NEX titles — it does not build on
[nextendo-nex](https://github.com/NextendoNetwork/nextendo-nex).

> **Measured responses are not part of this repository.** A handful of replies were originally
> established by observing the real service. Those bytes are Nintendo's and are not redistributable,
> so none of them are here. The code that used to embed them now loads them at start-up from a
> directory you supply yourself (`NPLN_CAPTURES_DIR`, default `.`), and every consumer degrades
> gracefully and says so in the log when a file is missing. See [`captures.go`](captures.go).
> Tests that replay a capture skip themselves rather than fail.

## Running

```sh
cp example.env .env    # then edit .env
go run .
```

Configuration is entirely through environment variables — see [`example.env`](example.env). No
secrets are baked into the source; the defaults point at loopback, never at a live deployment.

Set `NEXTENDO_SECRET` (or `NEXTENDO_SECRET_FILE`) before exposing this anywhere: it is what proves
a client's identity. Without it every session collapses to the same anonymous account.

## Splatfests

A Splatfest needs three things to name the *same* event: the schedule this server announces, the
decryption keys it hands out, and the BCAT delivery cache the console already holds. Announce a
fest whose packs the cache does not have and the game answers `BcatInvalid`.

Two hot-reloaded flags describe your cache: `festcache=<FESTID>` names the fest it holds, and
`festalias=<from>:<to>,…` rewrites the announced response to match it. Both empty — the default —
means the response is served exactly as built, with no rewriting. Your own keys go in `festcles`.

## Editing a player save

`cmd/saveedit` includes a local browser GUI for the server's `*.record.pb` CloudSave files. Choose a
player save, edit its `save_data` as formatted JSON, and save it back as protobuf. It also has CLI
commands to list, inspect, and directly patch records. Every edit creates a byte-for-byte
`.bak-<UTC timestamp>` copy, advances the protobuf `update_time`, writes through a synced temporary
file, and reads the result back to verify the protobuf before replacing the original. The GUI
preserves protobuf data outside the fields you edit and detects if the selected file changed after
you loaded it.

**Stop the NPLN server before editing.** It caches records in memory, so editing the file while the
process is running can be overwritten by a later game save. Stop the server, make the edits, restart
the server, then restart the game so it fetches the new cloud record.

Build and launch the GUI on the machine that has the save directory:

```sh
go build -o saveedit ./cmd/saveedit
./saveedit gui /data/saves
```

Open `http://127.0.0.1:8765/` in a browser. The editor listens on loopback only. `NPLN_SAVE_DIR` is
used when no directory argument is supplied; otherwise the default is `/data/saves`. Stop the
editor with Ctrl+C after editing, then restart the NPLN server and game.

The CLI is still available for quick one-off changes:

```sh
./saveedit list /data/saves
./saveedit show /data/saves/u-example.record.pb
./saveedit set /data/saves/u-example.record.pb Money 50000
```

For multiple fields, put their values in `changes.json` and apply them together:

```json
{
  "PlayerRank": 9,
  "Money": 50000,
  "WeaponLicense": 10
}
```

```sh
./saveedit patch /data/saves/u-example.record.pb changes.json
```

JSON integers stay 64-bit integers; strings, booleans, floats, arrays, objects and null are
supported. Special protobuf types can be entered with wrappers such as `{"$float32":1.5}` or
`{"$timestamp":"2026-09-24T20:00:00Z"}`. The `show` command prints the record using the same
wrappers, so a value can be copied back without changing its protobuf type. Nested map values use
dot paths, for example `SomeMap.SomeField`; sibling fields remain untouched.

## Clients

NPLN is spoken by the game, not by the console or the emulator: Splatoon 3 carries its own
statically linked gRPC stack and does TLS over raw sockets. What a client has to provide is three
things — it must resolve the NPLN hosts to this server, it must get past the game's own
certificate pinning, and it must present an identity this server's auth will accept.

[Ryujinx-Nextendo](https://github.com/NextendoNetwork/Ryujinx-Nextendo) is the only client that
does all three with no setup at all: the host redirection, the two built-in guest patches and the
signed account token are already wired in. A real console reaches the same place with DNS
redirection and the same patches applied — that is how this server was tested against hardware.
Anything else has to reproduce those three conditions on its own; a client that satisfies none of
them completes TLS and then abandons its own call before sending a single HTTP/2 HEADERS frame,
which the game surfaces as a communication error.

## What this is not

This server ships **no** Nintendo code, keys, measured data, or copyrighted assets. It is an
independent reimplementation for use with a community-run replacement service, not affiliated
with, endorsed by, or associated with Nintendo.

## License

Released under the **[PolyForm Shield License 1.0.0](LICENSE.md)** — source-available: read, use,
modify, and self-host, but do not use it to provide a product that competes with Nextendo Network.
