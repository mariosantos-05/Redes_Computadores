import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable, PeerEntry

async def main():
    print("--- Running Test 3: P2P Keep-Alive & RTT Tracking ---")
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
    bob_conn.keepalive_interval = 1.0  # short interval for fast test execution
    
    await bob_conn.connect("127.0.0.1", 5005)
    bob_listen_task = asyncio.create_task(bob_conn.listen())
    
    # Start Bob's keepalive loop
    bob_keepalive_task = asyncio.create_task(bob_conn.keepalive_loop())

    # Wait for a couple of pings to trigger and complete
    print("Waiting 4 seconds for PING/PONG exchanges and RTT calculations...")
    await asyncio.sleep(4.0)

    # Check RTT measurements
    print("RTT History in Bob's table for Alice:", alice_entry.rtt_history)
    print("Average RTT in Bob's table for Alice:", alice_entry.average_rtt)

    assert len(alice_entry.rtt_history) >= 2, "Should have recorded at least 2 RTT entries"
    assert alice_entry.average_rtt is not None, "Average RTT should not be None"
    assert alice_entry.average_rtt >= 0.0, "Average RTT should be non-negative"

    # Cleanup
    print("Cleaning up...")
    bob_keepalive_task.cancel()
    try:
        await bob_keepalive_task
    except asyncio.CancelledError:
        pass

    await bob_conn.close()
    bob_listen_task.cancel()
    try:
        await bob_listen_task
    except asyncio.CancelledError:
        pass

    for conn in list(alice_connections.values()):
        await conn.close()
        
    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    bob_server_runner.close()
    await bob_server_runner.wait_closed()

    print("Test 3 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
