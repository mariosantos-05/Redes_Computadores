# Project Checklist: P2P Chat

This document tracks implementation progress against the project requirements specified in the README.md.

---

## 1. Core Structures & Message Routing
* [x] Basic message framing and serialization (JSON + `\n`) — [message_router.py]
* [x] Peer tracking data structures (`PeerTable` and `PeerEntry`) — [peer_table.py]

---

## 2. Rendezvous Server Integration
* [x] Short-lived connection client for single-command queries — [rendezvous_connection.py]
* [x] Periodic background registration (`REGISTER` renewal before TTL expiration)
* [x] Periodic background discovery (`DISCOVER` loop updating local table)
* [x] Graceful cleanup on program termination (`UNREGISTER`)

---

## 3. P2P TCP Connection & Keep-Alive
* [x] Local TCP server (`PeerServer`) to listen for incoming peer connections
* [x] Outbound TCP client (`PeerConnection`) to initiate connections
* [x] presentation handshake (`HELLO` / `HELLO_OK`)
* [x] Periodic `PING` sending and keep-alive verification
* [x] Receiving `PING` and replying with `PONG`
* [x] RTT measurement and history averaging saved in `PeerEntry`
* [x] Connection closing on network timeout or failure

---

## 4. Chat Messaging Flow
* [x] Unicast direct messaging (`SEND` command)
* [x] Acknowledgment tracking (`require_ack` with `ACK` response)
* [x] Timeout handling for lost ACKs (warning logged after 5s)
* [x] Namespace broadcast (`PUB` with scope `#namespace`)
* [x] Global broadcast (`PUB` with scope `*`)

---

## 5. Controlled Session Termination
* [ ] Implement outbound `BYE` command detailing termination reason
* [ ] Implement inbound `BYE_OK` response and orderly socket closure

---

## 6. Resilience & Reconnections
* [x] Peer selection logic for reconnect candidates (ready after backoff)
* [x] Maximum reconnect threshold (`max_reconnect_attempts`) leading to `STALE` status
* [ ] Reconnection loop implementation (calling reconnect logic in background task)
* [ ] Exponential backoff reconciliation scheduling

---

## 7. Interactive CLI & Logging
* [ ] Interactive CLI loop (`cli.py` or integrated into `main.py`)
* [ ] Parse and execute CLI commands:
  - `/peers [* | #namespace]` (discover/list peers)
  - `/msg <peer_id> <message>` (unicast message)
  - `/pub * <message>` (global broadcast)
  - `/pub #<namespace> <message>` (namespace broadcast)
  - `/conn` (show active TCP connections)
  - `/rtt` (show average RTT per peer)
  - `/reconnect` (trigger manual peer reconciliation)
  - `/log <Level>` (change logging level dynamically)
  - `/quit` (clean close and exit)
* [ ] Setup dual-sink logging (stdout / console + persistent file `p2p.log`)
