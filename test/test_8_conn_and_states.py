import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable, PeerEntry

async def main():
    print("--- Running Test 8: P2P Connections Segregation, English States & Safe Shutdown ---")
    namespace = "TEST_NS"

    # 1. Setup Alice (Server / Inbound Connection receiver)
    alice_table = PeerTable()
    alice_inbound_connections = {}
    alice_outbound_connections = {}
    
    # Passing alice_inbound_connections as shared_connections to PeerServer
    alice_server = PeerServer("alice@TEST_NS", 5005, alice_table, alice_inbound_connections)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5005
    )

    # 2. Setup Bob (Client / Outbound Connection initiator)
    bob_table = PeerTable()
    bob_inbound_connections = {}
    bob_outbound_connections = {}
    
    # We populate the tables
    bob_entry = PeerEntry(name="bob", namespace=namespace, ip="127.0.0.1", port=5006, ttl=300)
    alice_table.peers[bob_entry.peer_id] = bob_entry
    
    alice_entry = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5005, ttl=300)
    bob_table.peers[alice_entry.peer_id] = alice_entry

    # 3. Connect Bob (outbound) to Alice (inbound)
    print("Bob connecting to Alice...")
    bob_conn = PeerConnection(
        "bob@TEST_NS",
        bob_table,
        on_close=lambda c: bob_outbound_connections.pop(c.remote_peer_id, None) if c.remote_peer_id else None
    )
    bob_conn.keepalive_interval = 99999
    
    await bob_conn.connect("127.0.0.1", 5005)
    bob_listen_task = asyncio.create_task(bob_conn.listen())
    bob_outbound_connections[bob_conn.remote_peer_id] = bob_conn

    # Allow connection processing
    await asyncio.sleep(0.5)

    # Verify Segregation:
    # Bob should have "alice@TEST_NS" in outbound_connections, and 0 in inbound_connections
    # Alice should have "bob@TEST_NS" in inbound_connections, and 0 in outbound_connections
    print("Verifying connection segregation...")
    assert "alice@TEST_NS" in bob_outbound_connections, "Alice should be in Bob's outbound connections"
    assert "alice@TEST_NS" not in bob_inbound_connections, "Alice should NOT be in Bob's inbound connections"
    assert "bob@TEST_NS" in alice_inbound_connections, "Bob should be in Alice's inbound connections"
    assert "bob@TEST_NS" not in alice_outbound_connections, "Bob should NOT be in Alice's outbound connections"
    print("Connection segregation verified successfully!")

    # Verify English State Names:
    # Ensure standard english status names are preserved in PeerEntry
    assert bob_entry.status == "CONNECTED", f"Expected CONNECTED, got {bob_entry.status}"
    assert alice_entry.status == "CONNECTED", f"Expected CONNECTED, got {alice_entry.status}"
    print("English state names verified successfully!")

    # Verify Safe Shutdown sequence logic (preventing RuntimeError):
    # Mimic the shutdown sequence from main.py
    print("Simulating safe shutdown sequence to ensure no RuntimeError occurs...")
    
    # Safely close Alice's inbound connections
    for pid, conn in list(alice_inbound_connections.items()):
        if isinstance(conn, str):
            continue
        print(f"Closing inbound conn to {pid}...")
        await conn.disconnect(reason="Shutdown")
        await conn.close()

    # Safely close Bob's outbound connections
    for pid, conn in list(bob_outbound_connections.items()):
        if isinstance(conn, str):
            continue
        print(f"Closing outbound conn to {pid}...")
        await conn.disconnect(reason="Shutdown")
        await conn.close()

    await asyncio.sleep(0.5)

    # Check that they were cleanly removed
    assert len(alice_inbound_connections) == 0, "Alice's inbound connections should be empty after close"
    assert len(bob_outbound_connections) == 0, "Bob's outbound connections should be empty after close"
    print("Safe shutdown completed without RuntimeError!")

    # Cleanup servers
    bob_listen_task.cancel()
    try:
        await bob_listen_task
    except asyncio.CancelledError:
        pass

    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    print("Test 8 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
