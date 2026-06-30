import asyncio
import sys
import os
import logging
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable, PeerEntry

class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record.getMessage())

async def main():
    print("--- Running Test 5: P2P Broadcast Messaging (PUB) ---")
    namespace = "TEST_NS"

    # Set up custom log capture handler
    router_logger = logging.getLogger("message_router")
    router_logger.setLevel(logging.INFO)
    log_capture = ListHandler()
    router_logger.addHandler(log_capture)

    # Set up Alice (the broadcaster)
    alice_table = PeerTable()
    alice_connections = {}
    alice_server = PeerServer("alice@TEST_NS", 5005, alice_table, alice_connections)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5005
    )

    # Set up Bob (receiver 1)
    bob_table = PeerTable()
    bob_connections = {}
    bob_server = PeerServer("bob@TEST_NS", 5006, bob_table, bob_connections)
    bob_server_runner = await asyncio.start_server(
        bob_server.handle_client,
        "127.0.0.1",
        5006
    )

    # Set up Charlie (receiver 2)
    charlie_table = PeerTable()
    charlie_connections = {}
    charlie_server = PeerServer("charlie@TEST_NS", 5007, charlie_table, charlie_connections)
    charlie_server_runner = await asyncio.start_server(
        charlie_server.handle_client,
        "127.0.0.1",
        5007
    )

    # Add Bob and Charlie to Alice's table
    bob_entry = PeerEntry(name="bob", namespace=namespace, ip="127.0.0.1", port=5006, ttl=300)
    charlie_entry = PeerEntry(name="charlie", namespace=namespace, ip="127.0.0.1", port=5007, ttl=300)
    alice_table.peers[bob_entry.peer_id] = bob_entry
    alice_table.peers[charlie_entry.peer_id] = charlie_entry

    # Add Alice to Bob's and Charlie's tables
    alice_entry_bob = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5005, ttl=300)
    bob_table.peers[alice_entry_bob.peer_id] = alice_entry_bob
    
    alice_entry_charlie = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5005, ttl=300)
    charlie_table.peers[alice_entry_charlie.peer_id] = alice_entry_charlie

    # Establish connections from Alice to Bob and Charlie
    conn_bob = PeerConnection("alice@TEST_NS", alice_table)
    conn_bob.keepalive_interval = 99999
    await conn_bob.connect("127.0.0.1", 5006)
    listen_bob = asyncio.create_task(conn_bob.listen())
    alice_connections[bob_entry.peer_id] = conn_bob

    conn_charlie = PeerConnection("alice@TEST_NS", alice_table)
    conn_charlie.keepalive_interval = 99999
    await conn_charlie.connect("127.0.0.1", 5007)
    listen_charlie = asyncio.create_task(conn_charlie.listen())
    alice_connections[charlie_entry.peer_id] = conn_charlie

    await asyncio.sleep(0.5)

    # 1. Namespace Broadcast
    msg_id_1 = str(uuid.uuid4())
    content_1 = "Hello namespace!"
    print("Alice publishing namespace broadcast...")
    for conn in alice_connections.values():
        await conn.publish(content=content_1, msg_id=msg_id_1, scope=f"#{namespace}")

    await asyncio.sleep(0.3)

    # 2. Global Broadcast
    msg_id_2 = str(uuid.uuid4())
    content_2 = "Hello world!"
    print("Alice publishing global broadcast...")
    for conn in alice_connections.values():
        await conn.publish(content=content_2, msg_id=msg_id_2, scope="*")

    await asyncio.sleep(0.3)

    # Print logs
    print("Logged router messages:")
    for record in log_capture.records:
        print(" >", record)

    # Verify that Bob logged receiving the namespace broadcast and global broadcast
    bob_ns_received = any(f"[PUB] [#{namespace}] alice@TEST_NS:" in record and content_1 in record for record in log_capture.records)
    bob_global_received = any("[PUB] [*] alice@TEST_NS:" in record and content_2 in record for record in log_capture.records)

    assert bob_ns_received, "Bob should have received the namespace broadcast"
    assert bob_global_received, "Bob should have received the global broadcast"
    print("Broadcast reception verified on both receivers.")

    # Cleanup
    print("Cleaning up...")
    router_logger.removeHandler(log_capture)

    for conn in list(alice_connections.values()):
        await conn.close()
    listen_bob.cancel()
    listen_charlie.cancel()
    try:
        await asyncio.gather(listen_bob, listen_charlie, return_exceptions=True)
    except Exception:
        pass

    # Clean up Bob and Charlie inbound connections if registered
    for conn in list(bob_connections.values()) + list(charlie_connections.values()):
        await conn.close()

    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    bob_server_runner.close()
    await bob_server_runner.wait_closed()
    charlie_server_runner.close()
    await charlie_server_runner.wait_closed()

    print("Test 5 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
