import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable, PeerEntry

async def main():
    print("--- Running Test 2: P2P Connection Handshake ---")
    namespace = "TEST_NS"

    # Set up Alice's server and table
    alice_table = PeerTable()
    alice_connections = {}
    alice_server = PeerServer("alice@TEST_NS", 5005, alice_table, alice_connections)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5005
    )
    print("Alice server started at 127.0.0.1:5005")

    # Set up Bob's server and table
    bob_table = PeerTable()
    bob_connections = {}
    bob_server = PeerServer("bob@TEST_NS", 5006, bob_table, bob_connections)
    bob_server_runner = await asyncio.start_server(
        bob_server.handle_client,
        "127.0.0.1",
        5006
    )
    print("Bob server started at 127.0.0.1:5006")

    # Add Bob's entry to Alice's table so Alice knows Bob's details
    bob_entry = PeerEntry(name="bob", namespace=namespace, ip="127.0.0.1", port=5006, ttl=300)
    alice_table.peers[bob_entry.peer_id] = bob_entry

    # Add Alice's entry to Bob's table so Bob knows Alice's details
    alice_entry = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5005, ttl=300)
    bob_table.peers[alice_entry.peer_id] = alice_entry

    # Establish P2P connection from Alice to Bob
    print("Alice initiating connection to Bob...")
    alice_conn = PeerConnection("alice@TEST_NS", alice_table)
    alice_conn.keepalive_interval = 99999
    
    await alice_conn.connect("127.0.0.1", 5006)
    alice_listen_task = asyncio.create_task(alice_conn.listen())
    
    # Wait for handshake processing (HELLO & HELLO_OK)
    await asyncio.sleep(0.5)

    # Verify states
    print("Checking connection states in tables...")
    print("Bob entry in Alice table:", bob_entry.status)
    print("Alice entry in Bob table:", alice_entry.status)

    assert bob_entry.status == "CONNECTED", "Bob should be CONNECTED in Alice's table"
    assert alice_entry.status == "CONNECTED", "Alice should be CONNECTED in Bob's table"
    
    # Cleanup
    print("Cleaning up...")
    await alice_conn.close()
    alice_listen_task.cancel()
    try:
        await alice_listen_task
    except asyncio.CancelledError:
        pass

    # Clean up Bob's inbound connections if registered
    for conn in list(bob_connections.values()):
        await conn.close()
        
    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    bob_server_runner.close()
    await bob_server_runner.wait_closed()

    print("Test 2 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
