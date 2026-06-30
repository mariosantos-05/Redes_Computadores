import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable, PeerEntry

async def main():
    print("--- Running Test 6: P2P Clean BYE/BYE_OK Termination ---")
    namespace = "TEST_NS"

    # Set up Alice (the server)
    alice_table = PeerTable()
    alice_connections = {}
    alice_server = PeerServer("alice@TEST_NS", 5005, alice_table, alice_connections)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5005
    )

    # Set up Bob (the client)
    bob_table = PeerTable()
    bob_connections = {}
    bob_server = PeerServer("bob@TEST_NS", 5006, bob_table, bob_connections)
    bob_server_runner = await asyncio.start_server(
        bob_server.handle_client,
        "127.0.0.1",
        5006
    )

    # Bob's entry for Alice
    alice_entry = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5005, ttl=300)
    bob_table.peers[alice_entry.peer_id] = alice_entry

    # Alice's entry for Bob
    bob_entry = PeerEntry(name="bob", namespace=namespace, ip="127.0.0.1", port=5006, ttl=300)
    alice_table.peers[bob_entry.peer_id] = bob_entry

    # Connect Bob to Alice
    bob_conn = PeerConnection("bob@TEST_NS", bob_table)
    bob_conn.keepalive_interval = 99999
    
    await bob_conn.connect("127.0.0.1", 5005)
    bob_listen_task = asyncio.create_task(bob_conn.listen())
    
    await asyncio.sleep(0.5)
    assert alice_entry.status == "CONNECTED", "Alice should be CONNECTED before BYE"

    # Trigger clean disconnect from Bob
    print("Bob sending BYE message to Alice...")
    await bob_conn.disconnect("Graceful teardown")

    # Wait for the BYE/BYE_OK handshake to complete and socket to close
    await asyncio.sleep(0.5)

    print("Checking connection statuses after BYE...")
    print("Alice entry status in Bob's table:", alice_entry.status)
    print("Bob entry status in Alice's table:", bob_entry.status)

    assert alice_entry.status == "DISCONNECTED", "Alice should be marked DISCONNECTED in Bob's table"
    assert bob_entry.status == "DISCONNECTED", "Bob should be marked DISCONNECTED in Alice's table"

    # Cleanup remaining servers
    bob_listen_task.cancel()
    try:
        await bob_listen_task
    except asyncio.CancelledError:
        pass

    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    bob_server_runner.close()
    await bob_server_runner.wait_closed()

    print("Test 6 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
