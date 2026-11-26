# Changes: concurrency, expiry and binary-safety fixes

Load testing the original server with `redis-benchmark` (50 parallel clients) showed
three classes of problems. This change fixes them and adds a test suite.

| Problem (before) | Evidence | Fix |
|---|---|---|
| No active expiry: a key with a TTL that was never read again stayed in memory forever | No timer or background thread anywhere | Background expiry cycle modelled on Redis |
| `INCR` lost updates | 100,000 parallel INCRs ended at 98,215 | Atomic read-modify-write with `ConcurrentHashMap.compute` |
| Parallel `LPOP` crashed connections | All 50 clients died with `NoSuchElementException` | Pop's check and removal done under the list lock |
| Lists lost pushes; replica diverged | 100,000 LPUSH → 99,814 on master, 99,815 on replica | List creation via `compute`, push under lock, propagate inside the lock |
| Not binary safe | Sent bytes `FF FE`, got back `EF BF BD EF BF BD` | Byte-transparent ISO-8859-1 codec instead of UTF-8 |

After the fix, the same benchmark gives exactly 100,000 for INCR, an empty list on both
master and replica after 100k LPUSH + 100k LPOP, no exceptions, and `FF FE` round-trips.

## 1. Active TTL expiry (`core/RedisDatabase.java`)

- A single daemon thread (`ScheduledExecutorService`) runs `activeExpireCycle()` every 100 ms.
- Each run checks up to **20 keys that have a TTL** and deletes the expired ones. If **more than
  25%** of the sample was expired, it assumes many more are dead and repeats, until a **25 ms
  time budget** runs out. These are Redis's defaults (`hz 10`, `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP 20`).
- Only keys with a TTL are scanned: they are tracked in a separate concurrent set
  (`volatileKeys`, like Redis's `expires` dict), so keys without a TTL cost nothing.
- The scan uses a cursor that persists between runs (as Redis 6+ does) instead of pure random
  picks. Every key is visited eventually, and each run does bounded work.
- Deletion uses `store.remove(key, entry)`, so it only removes that exact expired entry and
  never a fresh value another client just wrote under the same name. The same fix was applied
  to passive expiry in `get()` and to the `KEYS` sweep.
- Passive expiry (check on every read) is unchanged.

## 2. Race fixes (`core/RedisDatabase.java`, `commands/CommandDispatcher.java`)

- **INCR**: new `incrBy()` does the whole parse, add and store inside
  `ConcurrentHashMap.compute`, which holds the lock for that key's bin. It also keeps
  the key's TTL (like Redis) and reports overflow (`Math.addExact`).
- **SET NX / XX**: `setStringIfAbsent` / `setStringIfPresent` use `compute`, so check and
  write are one step. 16 threads racing `SET NX` now produce exactly one winner.
- **Lists**: every list read and write (`LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/BLPOP`) now runs
  under the database's wait lock, which BLPOP already used. `getOrCreateList` uses
  `compute`, so two threads can't both create a list.
- **Streams**: `XRANGE` now reads the `TreeMap` under the same lock that `XADD` writes under.
- **Replication order**: list and stream writes call `maybePropagate` inside the lock, so
  replicas receive them in the same order the master applied them. The `WAIT` command's
  `GETACK` write is now under the same lock as `propagate()`, so it can't be spliced into
  the middle of another command on the replica socket.
- **BLPOP / XREAD** now write their reply after releasing the lock, so a slow client can't
  stall every writer.
- Any unexpected `RuntimeException` in a command becomes an `ERR` reply instead of killing the
  connection thread.

## 3. Binary-safe values (`protocol/RespReader.java`, `RespWriter.java`, `rdb/RdbLoader.java`)

Values are still Java `String`s, but they are now decoded and encoded with **ISO-8859-1**
instead of UTF-8. ISO-8859-1 maps each byte 0x00 to 0xFF to exactly one `char` and back, so
any byte sequence (images, serialized objects, invalid UTF-8) round-trips unchanged. UTF-8 text
from clients also round-trips, because its bytes pass through untouched. Lengths stay
correct because one char equals one byte.

## 4. Buffered input (`server/ClientHandler.java`, `replication/ReplicaClient.java`)

The parser calls `read()` once per byte. On a raw socket stream each of those calls was a
syscall. Both connection types now read through a 16 KB `BufferedInputStream`.

On the replica, the handshake reader and the replication-stream reader share **one** buffered
stream. Separate buffers would let the first one read ahead and swallow propagated commands.
`CountingInputStream` wraps the buffer rather than the socket, so it counts the bytes the parser
consumed, not the bytes the buffer prefetched. That keeps the replication offset exact, and a
test asserts master and replica offsets match after `WAIT`.

## 5. Tests (`src/test/java`, run with `mvn test`)

23 JUnit 5 tests, all passing:

- `RespReaderTest`: arrays, CRLF inside a value, inline commands, frames delivered one byte per
  read, all 256 byte values round-trip, RDB payload without trailing CRLF.
- `RedisDatabaseTest`: passive expiry; active cycle deletes 500 unread expired keys and leaves
  others alone; the cycle repeats when >25% expired; the background thread expires 1,000 keys
  with no reads; 16 threads × 10,000 INCR = 160,000; INCR keeps TTL and rejects non-integers
  and overflow; one list per key under contention; exactly one `SET NX` winner.
- `ServerIntegrationTest` (real TCP): basic commands, binary and UTF-8 values, 8 clients ×
  2,000 INCR, 8 pushers + 8 poppers with no errors and no lost items, active expiry over the
  wire, BLPOP wake-up, and a live master + replica with `WAIT 1` and matching offsets.

## Benchmarks

`redis-benchmark`, 4-core Linux VM, 50 clients, two rounds each, run back-to-back against the
old build, the new build and Redis 7.0.15 on the same machine:

| | Old build | New build | Redis 7.0 |
|---|---|---|---|
| SET / GET, no pipelining | 54k to 65k ops/s | 56k to 64k ops/s | 108k to 129k ops/s |
| SET / GET, pipeline 16 | 277k to 370k ops/s | **542k to 583k ops/s** | 1.6M to 1.75M ops/s |

Pipelined throughput roughly doubled because of the buffered reader. Unpipelined throughput is
unchanged within noise. There it's bounded by one round trip per request and thread context
switches, not by parsing.

## Still not done (by design, see README)

- Full resync still sends an empty RDB, and there's no backlog for partial resync.
- `MULTI/EXEC` is not isolated from other clients, and `WATCH` is a no-op.
- Plain `SET` and `DEL` from different clients can still reach replicas in a different order
  than the master applied them. The full fix is a single command-execution thread, like Redis.
- RDB loading: strings only, and LZF-compressed strings are not decompressed.
- Blocking waiters are woken with `notifyAll` on one lock (no per-key FIFO queues).
