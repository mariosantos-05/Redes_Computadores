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
    print("--- Running Test 4: P2P Direct SEND & ACK Messaging ---")
    namespace = "TEST_NS"

    # Set up custom log capture handler
    router_logger = logging.getLogger("message_router")
    router_logger.setLevel(logging.INFO)
    log_capture = ListHandler()
    router_logger.addHandler(log_capture)

    # Set up Alice (the server/receiver)
    alice_table = PeerTable()
    alice_connections = {}
    alice_server = PeerServer("alice@TEST_NS", 5005, alice_table, alice_connections)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5005
    )

    # Set up Bob (the client/sender)
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
    bob_conn.keepalive_interval = 99999  # no keep-alive pings during this test
    
    await bob_conn.connect("127.0.0.1", 5005)
    bob_listen_task = asyncio.create_task(bob_conn.listen())
    
    await asyncio.sleep(0.5)

    # Send a message requiring ACK
    msg_id = str(uuid.uuid4())
    msg_content = "Hello, Alice! This is Bob."
    print(f"Bob sending message to Alice (msg_id={msg_id})...")
    
    # We await send_message. Since require_ack is True, it will block until ACK is received or timeout (5s)
    await bob_conn.send_message(content=msg_content, msg_id=msg_id, require_ack=True)

    # Wait a moment for any remaining processing
    await asyncio.sleep(0.2)

    # Verify Alice received the message
    print("Logged router messages:")
    for record in log_capture.records:
        print(" >", record)

    # Check if Alice logged receiving the message
    received = any(
        "[SEND]" in record and "bob@TEST_NS" in record and msg_content in record
        for record in log_capture.records
    )
    assert received, "Alice should have received and logged the SEND message"
    print("Message reception verified on Alice's side.")

    # Cleanup
    print("Cleaning up...")
    router_logger.removeHandler(log_capture)

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

    print("Test 4 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
