# PA2: Leader Election on an Asynchronous Ring

This program elects a single leader among processes arranged in a
unidirectional ring. It implements the \(O(n^2)\) Chang-Roberts / LCR
algorithm described in the [asynchronous ring section of the Wikipedia
leader-election article](https://en.wikipedia.org/wiki/Leader_election#Asynchronous_ring).

Each process has a unique `uuid.uuid4()` identity. Messages travel
**client → server** around the ring. A node forwards a UUID only if it
is **greater** than its own, **ignores** a smaller UUID, and if it
receives **its own** UUID it becomes the leader and announces `flag=1`.
Every node ends with the same `leader_id`.

## Files

| File | Purpose |
| --- | --- |
| `myleprocess.py` | Ring process (server + client + election) |
| `config.txt` | Two-line `ip,port` file (listen, then connect) |
| `log1.txt` `log2.txt` `log3.txt` | Logs from the local 3-node demo |

## Requirements

- Python 3.9+
- No third-party packages

## `config.txt`

```
<your_ip>,<your_listen_port>
<neighbor_ip>,<neighbor_port>
```

- **Line 1** is this node acting as a **server** (bind / listen / accept).
- **Line 2** is the neighbor this node connects to as a **client**.

The checked-in file is the first node of a localhost demo:

```
127.0.0.1,5001
127.0.0.1,5002
```

For the in-class demo, replace those addresses with the pair you exchange
in class.

## How to run one process

From the `pa2` directory:

```bash
python3 myleprocess.py --config config.txt --log log.txt
```

Useful flags:

| Flag | Meaning |
| --- | --- |
| `--config PATH` | Config file (default: `config.txt`) |
| `--log PATH` | Log file (default: `log.txt`) |
| `--wait` | After `accept()`, wait for Enter before `connect()` |

`accept()` runs in a **separate thread** from `connect()` so the ring
cannot deadlock. After both sockets are up, the node sends its UUID
once, then only reacts to messages (`recv` → compare → forward/ignore).

TCP connections are left open after the leader is known.

## In-class ring (with another student)

1. Put your listen address on line 1 of `config.txt`.
2. Put the other student's address on line 2.
3. Start your process. Optional barrier so everyone accepts first:

```bash
python3 myleprocess.py --config config.txt --log log.txt --wait
```

4. When every node is listening, press Enter. Each client then connects
   and the election runs.

Messages are newline-delimited JSON:

```json
{"uuid": "123e4567-e89b-42d3-a456-556642440000", "flag": 0}
```

`flag` is `0` during election and `1` once a leader is announced.

## Local demo: three processes on one machine

Open **three terminals** in `pa2`. The ring is:

```
Node 1 listen 5001 → connect 5002
Node 2 listen 5002 → connect 5003
Node 3 listen 5003 → connect 5001
```

**Terminal 1**

```bash
printf '127.0.0.1,5001\n127.0.0.1,5002\n' > config.txt
python3 myleprocess.py --config config.txt --log log1.txt
```

**Terminal 2**

```bash
printf '127.0.0.1,5002\n127.0.0.1,5003\n' > config2.txt
python3 myleprocess.py --config config2.txt --log log2.txt
```

**Terminal 3**

```bash
printf '127.0.0.1,5003\n127.0.0.1,5001\n' > config3.txt
python3 myleprocess.py --config config3.txt --log log3.txt
```

Start all three within a few seconds of each other. Each process retries
`connect()` until the neighbor's server is up.

## Execution example (local 3-node demo)

All three nodes agreed on leader
`a328176e-1380-4b29-90f3-2790f0c145f0`.

### Terminal 1 (`127.0.0.1:5001` → `:5002`)

```
Process id: a328176e-1380-4b29-90f3-2790f0c145f0
Listening on 127.0.0.1:5001, connecting to 127.0.0.1:5002
Both connections established.
Sent: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=0
Received: uuid=9ba0277c-ad76-4d95-a5f7-ebbc8877b12b, flag=0, less, 0
Ignored: uuid=9ba0277c-ad76-4d95-a5f7-ebbc8877b12b, flag=0 (smaller uuid)
Received: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=0, same, 0
Leader is decided to a328176e-1380-4b29-90f3-2790f0c145f0.
leader is a328176e-1380-4b29-90f3-2790f0c145f0
Sent: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1
Received: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1, same, 1, leader=a328176e-1380-4b29-90f3-2790f0c145f0
Ignored: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1 (leader already known)
```

### Terminal 2 (`127.0.0.1:5002` → `:5003`)

```
Process id: 52c714c5-05fa-4dfc-ae66-f6b5ef63f241
Listening on 127.0.0.1:5002, connecting to 127.0.0.1:5003
Both connections established.
Sent: uuid=52c714c5-05fa-4dfc-ae66-f6b5ef63f241, flag=0
Received: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=0, greater, 0
Sent: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=0
Received: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1, greater, 0
Leader is decided to a328176e-1380-4b29-90f3-2790f0c145f0.
leader is a328176e-1380-4b29-90f3-2790f0c145f0
Sent: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1
```

### Terminal 3 (`127.0.0.1:5003` → `:5001`)

```
Process id: 9ba0277c-ad76-4d95-a5f7-ebbc8877b12b
Listening on 127.0.0.1:5003, connecting to 127.0.0.1:5001
Both connections established.
Sent: uuid=9ba0277c-ad76-4d95-a5f7-ebbc8877b12b, flag=0
Received: uuid=52c714c5-05fa-4dfc-ae66-f6b5ef63f241, flag=0, less, 0
Ignored: uuid=52c714c5-05fa-4dfc-ae66-f6b5ef63f241, flag=0 (smaller uuid)
Received: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=0, greater, 0
Sent: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=0
Received: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1, greater, 0
Leader is decided to a328176e-1380-4b29-90f3-2790f0c145f0.
leader is a328176e-1380-4b29-90f3-2790f0c145f0
Sent: uuid=a328176e-1380-4b29-90f3-2790f0c145f0, flag=1
```

After the `flag=1` announcement circles the ring, nobody sends further
election messages. Each process still holds the TCP connections open
(Ctrl+C to stop a local run).
