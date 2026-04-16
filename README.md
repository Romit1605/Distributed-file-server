# Distributed File Server

A multi-process file server in C using TCP sockets, with three load-balanced mirror servers and a fork-per-client concurrency model. Supports directory listing, file search, and tar-bundled file retrieval by size, extension, or date.

## Overview

This project implements a distributed client-server file service where multiple clients can concurrently query a server for files in its home directory tree. The system consists of three identical server processes — a primary gateway and two mirror replicas — that share incoming load through a coordinated routing protocol.

The client connects to the gateway, which either keeps the connection or transparently redirects it to one of the mirrors based on a global connection counter. Once connected, the client can issue commands to list directories, look up file metadata, or retrieve filtered sets of files as compressed tar archives.

## Features

- **Three load-balanced server processes** — one gateway plus two mirrors
- **Two-phase routing strategy** — pairs of connections for the first six clients, single round-robin thereafter
- **Fork-per-client concurrency** — each session runs in an isolated child process
- **Length-framed wire protocol** — reliable message boundaries over a TCP byte stream
- **Distributed coordination via flock()** — atomic counter updates across three processes
- **Eight client commands** — directory listing, file search, and tar-bundled retrieval by size, extension, or date
- **Client-side syntax validation** — invalid commands rejected before reaching the server
- **Automatic redirect handling** — client transparently follows REDIRECT responses
- **Chunked file transfer with progress bar** — 4 KB chunks with `\r`-repainted ASCII bar
- **SIGCHLD-based zombie reaping** — no orphaned processes accumulate

## Architecture

┌─────────────┐
                    │   client    │
                    └──────┬──────┘
                           │ TCP :19081
                           ▼
                    ┌─────────────┐         routing
                    │  w26server  │◄────────decisions
                    │  (gateway)  │         via flock
                    └──────┬──────┘         counter file
                           │
              REDIRECT ────┼──── REDIRECT
              to :19082    │     to :19083
                   │       │       │
                   ▼       ▼       ▼
              ┌─────────┐  │  ┌─────────┐
              │ mirror1 │  │  │ mirror2 │
              └─────────┘  │  └─────────┘
                           │
                   (or kept locally,
                    forks crequest())

### File responsibilities

| File | Role |
|---|---|
| `w26server.c` | Gateway and primary worker. Accepts every client first, decides routing, either forks a child or sends REDIRECT. |
| `mirror1.c`   | Secondary worker. Listens on its own port; identical command set, differently structured internals. |
| `mirror2.c`   | Tertiary worker. Same role as mirror1 on a different port. |
| `client.c`    | Interactive client with FSM tokenizer, per-command validators, framed I/O, and progress bar. |

## Load Balancing

The system follows a strict two-phase routing pattern:

**Phase A (connections 1–6): paired distribution**

| Connection | Handler  |
|------------|----------|
| 1, 2       | server   |
| 3, 4       | mirror1  |
| 5, 6       | mirror2  |

**Phase B (connections 7+): single round-robin**

| Connection | Handler  |
|------------|----------|
| 7          | server   |
| 8          | mirror1  |
| 9          | mirror2  |
| 10         | server   |
| 11         | mirror1  |
| 12         | mirror2  |
| ...        | ...      |

The connection count is the byte size of `/tmp/w26_global_counter`. Every connection appends one byte under `flock(LOCK_EX)`, ensuring atomic updates across the three server processes. No parsing, no formatting errors, no race conditions.

## Wire Protocol

All messages are length-framed for reliable parsing over a TCP byte stream:

| Tag    | Direction       | Body                          |
|--------|-----------------|-------------------------------|
| `TEXT` | server → client | Text response (e.g. file list)|
| `FILE` | server → client | Raw bytes of a tar.gz archive |
| `ERR`  | server → client | Human-readable error message  |
| `BYE`  | server → client | Acknowledgement of `quitc`    |

Initial greeting is unframed: `OK MAIN\n`, `OK MIRROR1\n`, `OK MIRROR2\n`, or `REDIRECT host port\n`.

## Commands

| Command                      | Description                                                  |
|------------------------------|--------------------------------------------------------------|
| `dirlist -a`                 | Subdirectories of `$HOME` in alphabetical order              |
| `dirlist -t`                 | Subdirectories of `$HOME` in creation time order             |
| `fn <filename>`              | Metadata for the first matching file in the tree             |
| `fz <size1> <size2>`         | Tar all files with `size1 ≤ size ≤ size2`                    |
| `ft <ext1> [ext2] [ext3]`    | Tar all files with one of the given extensions (1–3 allowed) |
| `fdb <YYYY-MM-DD>`           | Tar all files modified on or before the given date           |
| `fda <YYYY-MM-DD>`           | Tar all files modified on or after the given date            |
| `quitc`                      | End the session                                              |

Returned tar archives are saved to `~/project/temp.tar.gz` on the client side.

## Build

```bash
gcc -Wall -O2 -o w26server w26server.c
gcc -Wall -O2 -o mirror1   mirror1.c
gcc -Wall -O2 -o mirror2   mirror2.c
gcc -Wall -O2 -o client    client.c
```

Tested on Linux with GCC 11+ and a POSIX-compliant environment. No external dependencies beyond libc and standard system headers.

## Run

Start each server in its own terminal:

```bash
./w26server   # terminal 1
./mirror1     # terminal 2
./mirror2     # terminal 3
```

Then connect a client:

```bash
./client      # terminal 4
```

Example session:
[client] connected: OK MAIN
w26client$ dirlist -a
alpha
beta
gamma
w26client$ fn readme.txt
Filename   : readme.txt
Size       : 20 bytes
Created    : 2026-02-10 11:56:13
Permissions: 644
w26client$ fz 1000 5000
Receiving 3834 bytes into /home/user/project/temp.tar.gz
[##############################] 100%  (3834 / 3834 bytes)
Saved: /home/user/project/temp.tar.gz
w26client$ quitc
## Implementation Notes

Each server file uses **structurally distinct implementations** of shared algorithms — same protocol, three different internal designs.

| Function | w26server | mirror1 | mirror2 |
|---|---|---|---|
| Per-session FSM | Transition table | Mutual recursive descent | Jump table |
| `dirlist -a` sort | Bottom-up merge sort | Top-down recursive merge sort | Bottom-up merge sort |
| `dirlist -t` sort | LSD radix sort | DLL insertion sort | Binary min-heap |
| File search (`fn`) | Iterative DFS, array stack | BFS, circular queue | Iterative DFS, linked-list stack |
| Predicate walker | Streaming to file | In-memory path vector | Two-phase inode set |
| `recv_line` | Per-fd slot table, sliding window | Per-fd slot table, head/length | Chunked `MSG_PEEK` + `memchr` |
| `send_all` | Pointer arithmetic | Index-based offset | `do-while` cursor |

This was a deliberate choice to demonstrate algorithmic variety while preserving identical externally observable behavior.

## Concurrency Model

Each server uses the classic **process-per-connection** model:
parent (listening on port)
│
├── accept() returns new socket
│
├── fork() → child runs crequest() exclusively for that client
│
└── parent immediately returns to accept()
Children are reaped via a `SIGCHLD` handler in the parent that calls `waitpid(-1, NULL, WNOHANG)`. Each child resets its own `SIGCHLD` to `SIG_DFL` so that explicit `waitpid()` calls inside `create_tar()` are not racing with an inherited reap handler.

## Tested Scenarios

- All eight commands with valid and invalid inputs
- All client-side syntax validators (count mismatches, bad date format, non-numeric sizes, etc.)
- Connection rotation across 12+ consecutive clients (verified textbook pattern)
- Concurrent multi-client sessions with non-overlapping directory operations
- Mid-transfer client disconnect (server child exits cleanly, parent reaps)
- Server restart with stale counter state (cleaned via `rm /tmp/w26_global_counter`)
- Large file transfers (multi-megabyte tarballs) with progress bar correctness

## Project Structure
.
├── w26server.c    # Gateway and primary worker
├── mirror1.c      # Secondary worker
├── mirror2.c      # Tertiary worker
├── client.c       # Interactive client
└── README.md      # This file
## Built With

- **Language:** C (C11)
- **Networking:** POSIX TCP sockets (`AF_INET`, `SOCK_STREAM`)
- **Concurrency:** `fork()`, `waitpid()`, `SIGCHLD`
- **Coordination:** `flock()` for distributed counter
- **Compression:** `tar` with `gzip` (invoked via `execlp`)
- **Build:** GCC with `-Wall -O2`

## License

This project was developed as coursework for COMP-8567 (Advanced Systems Programming) at the University of Windsor, Winter 2026.

## Author

Romit Patel — Master of Applied Computing, University of Windsor

client
    |
    | TCP :19081
    v
  w26server (gateway)
    |
    +-- forks a child for itself, OR
    +-- sends REDIRECT to :19082 (mirror1)
    +-- sends REDIRECT to :19083 (mirror2)

Coordination: flock-protected counter file in /tmp
