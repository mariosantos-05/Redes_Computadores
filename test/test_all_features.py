import asyncio
import subprocess
import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rendezvous_connection import RendezvousConnection
from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable, PeerEntry

# Global Rendezvous settings for the test
RDV_HOST = "127.0.0.1"
RDV_PORT = 8585

async def run_test_suite():
    print("======================================================================")
    print("🧪 RUNNING COMPREHENSIVE P2P & RENDEZVOUS TEST SUITE")
    print("======================================================================")
    
    # 1. Start local Rendezvous Server
    print("\n[RDV] Starting local Rendezvous Server on port 8585...")
    rdv_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pyp2p-rdv", "src", "rendezvous", "main.py"))
    if not os.path.exists(rdv_script):
        print(f"❌ Test Failed: Rendezvous script not found at {rdv_script}")
        sys.exit(1)
        
    rdv_proc = subprocess.Popen(
        [sys.executable, rdv_script, "--host", RDV_HOST, "--port", str(RDV_PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    await asyncio.sleep(1.5)
    print("✔ Rendezvous Server is up and running.")

    rdv_conn = RendezvousConnection(RDV_HOST, RDV_PORT)
    success = True

    # Initialize all resources/tasks to None for cleanup in finally block
    bob_conn = None
    conn_A_to_B = None
    conn_A_to_C = None
    conn_B_to_C = None
    alice_server_task = None
    nodeA_server_task = None
    nodeB_server_task = None
    nodeC_server_task = None
    msg_server_task = None
    msg_conn = None
    rec2_task = None
    rec3_task = None
    msg_conn2 = None
    msg_conn3 = None

    try:
        # --------------------------------------------------------------------
        # TEST 1: Rendezvous REGISTER
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("📝 TEST 1: Rendezvous REGISTER")
        print("----------------------------------------------------------------------")
        reg_alice = {
            "type": "REGISTER",
            "namespace": "TEST_NS",
            "name": "alice",
            "port": 5005,
            "ttl": 30
        }
        print(f"Sending REGISTER: {reg_alice}")
        response = await rdv_conn.request(reg_alice)
        print(f"Received Response: {response}")
        
        assert response.get("status") == "OK", "Register status should be OK"
        assert response.get("port") == 5005, "Returned port should match registered port"
        print("✔ TEST 1 PASSED: Registration completed successfully.")

        # --------------------------------------------------------------------
        # TEST 2: Rendezvous DISCOVER
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("🔍 TEST 2: Rendezvous DISCOVER")
        print("----------------------------------------------------------------------")
        disc_msg = {
            "type": "DISCOVER",
            "namespace": "TEST_NS"
        }
        print(f"Sending DISCOVER: {disc_msg}")
        response = await rdv_conn.request(disc_msg)
        print(f"Received Response: {response}")
        
        assert response.get("status") == "OK", "Discover status should be OK"
        peers = response.get("peers", [])
        assert len(peers) == 1, "Should discover exactly 1 peer (alice)"
        assert peers[0]["name"] == "alice", "Discovered peer name should be 'alice'"
        print("✔ TEST 2 PASSED: Peer discovery verified.")

        # --------------------------------------------------------------------
        # TEST 3: P2P Handshake (HELLO / HELLO_OK)
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("🤝 TEST 3: P2P TCP Handshake")
        print("----------------------------------------------------------------------")
        
        # Start Alice's P2P Server
        alice_table = PeerTable()
        # Add Bob to Alice's table so she updates Bob's connection state
        bob_entry_in_alice = PeerEntry(name="bob", namespace="TEST_NS", ip="127.0.0.1", port=5006, ttl=300)
        alice_table.peers[bob_entry_in_alice.peer_id] = bob_entry_in_alice
        
        alice_server = PeerServer(peer_id="alice@TEST_NS", port=5005, peer_table=alice_table)
        alice_server_task = asyncio.create_task(alice_server.start())
        await asyncio.sleep(0.5)
        print("Alice P2P Server started listening on port 5005.")

        # Create Bob's connection to Alice
        bob_table = PeerTable()
        alice_entry_in_bob = PeerEntry(name="alice", namespace="TEST_NS", ip="127.0.0.1", port=5005, ttl=300)
        bob_table.peers[alice_entry_in_bob.peer_id] = alice_entry_in_bob
        
        bob_conn = PeerConnection(peer_id="bob@TEST_NS", peer_table=bob_table)
        bob_conn.keepalive_interval = 2  # Speed up keepalive for testing
        
        print("Bob connecting to Alice at 127.0.0.1:5005...")
        await bob_conn.connect("127.0.0.1", 5005)
        bob_listen_task = asyncio.create_task(bob_conn.listen())
        
        # Give a small moment for handshake to sync
        await asyncio.sleep(0.2)
        
        assert bob_conn.remote_peer_id == "alice@TEST_NS", "Bob's remote peer ID should be alice@TEST_NS"
        assert alice_entry_in_bob.status == "CONNECTED", "Alice should be CONNECTED in Bob's table"
        assert bob_entry_in_alice.status == "CONNECTED", "Bob should be CONNECTED in Alice's table"
        print("✔ TEST 3 PASSED: Bidirectional handshake and PeerTable connection states verified.")

        # --------------------------------------------------------------------
        # TEST 4: P2P Keep-Alive & RTT Calculation
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("⏱ TEST 4: P2P Keep-Alive & RTT Calculation")
        print("----------------------------------------------------------------------")
        print("Waiting for keepalive PING/PONG exchanges...")
        await asyncio.sleep(5)  # Let it run for 2 keepalive intervals (interval = 2s)
        
        print(f"Measured RTT history: {alice_entry_in_bob.rtt_history}")
        print(f"Average RTT: {alice_entry_in_bob.average_rtt} ms")
        
        assert len(alice_entry_in_bob.rtt_history) >= 2, "Should have recorded at least 2 RTT entries"
        assert alice_entry_in_bob.average_rtt is not None, "Average RTT should be calculated"
        assert alice_entry_in_bob.average_rtt > 0, "Average RTT should be positive"
        print("✔ TEST 4 PASSED: Periodic PING/PONG keep-alives and RTT average tracking verified.")

        # --------------------------------------------------------------------
        # TEST 5: Timeout & Error Handling (Clean Closure)
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("🔌 TEST 5: Timeout & Error Handling (Clean Closure)")
        print("----------------------------------------------------------------------")
        print("Simulating Alice disconnecting...")
        
        # Stop Alice's server and task
        alice_server_task.cancel()
        try:
            await alice_server_task
        except asyncio.CancelledError:
            pass
            
        print("Alice P2P Server stopped. Waiting for Bob to detect disconnect...")
        await asyncio.sleep(1.0)
        
        assert alice_entry_in_bob.status == "DISCONNECTED", "Alice should be marked DISCONNECTED in Bob's table after connection drops"
        print("✔ TEST 5 PASSED: Unexpected disconnect successfully detected and cleaned up.")

        # --------------------------------------------------------------------
        # TEST 6: Rendezvous UNREGISTER
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("🚪 TEST 6: Rendezvous UNREGISTER")
        print("----------------------------------------------------------------------")
        unreg_msg = {
            "type": "UNREGISTER",
            "namespace": "TEST_NS",
            "name": "alice",
            "port": 5005
        }
        print(f"Sending UNREGISTER: {unreg_msg}")
        response = await rdv_conn.request(unreg_msg)
        print(f"Received Response: {response}")
        
        assert response.get("status") == "OK", "Unregister status should be OK"
        
        # Discovery query to verify removal
        response = await rdv_conn.request(disc_msg)
        peers = response.get("peers", [])
        assert len(peers) == 0, "Alice should be removed from discovered peers list"
        print("✔ TEST 6 PASSED: Clean unregistration verified.")
        # --------------------------------------------------------------------
        # TEST 7: Background Registration Loop
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("🔄 TEST 7: Background Registration Loop")
        print("----------------------------------------------------------------------")
        # Register Charlie manually first
        await rdv_conn.request({"type": "REGISTER", "namespace": "TEST_NS", "name": "charlie", "port": 7001, "ttl": 10})
        
        # Start registration loop for Charlie with a short TTL (10s), re-registering every 8s
        charlie_reg_task = asyncio.create_task(
            rdv_conn.registration_loop(
                namespace="TEST_NS",
                name="charlie",
                port=7001,
                initial_ttl=10
            )
        )
        print("Charlie registration loop started in background (TTL = 10s).")
        await asyncio.sleep(1.0) # wait for loop to start and sleep
        
        # Verify Charlie is registered
        disc_charlie = await rdv_conn.request({"type": "DISCOVER", "namespace": "TEST_NS"})
        charlie_present = any(p["name"] == "charlie" for p in disc_charlie.get("peers", []))
        assert charlie_present, "Charlie should be registered initially"
        print("✔ Charlie registration verified.")

        print("Waiting 9 seconds for re-registration loop to trigger...")
        await asyncio.sleep(9.0)
        
        # Verify Charlie is still registered and has refreshed TTL
        disc_charlie = await rdv_conn.request({"type": "DISCOVER", "namespace": "TEST_NS"})
        charlie_peer = next((p for p in disc_charlie.get("peers", []) if p["name"] == "charlie"), None)
        assert charlie_peer is not None, "Charlie should still be registered after 10s"
        assert charlie_peer["expires_in"] > 5, f"Charlie's TTL should have refreshed, got expires_in={charlie_peer['expires_in']}"
        print(f"✔ Re-registration verified. Charlie expires in {charlie_peer['expires_in']}s.")
        
        # Cancel the loop
        charlie_reg_task.cancel()
        await asyncio.gather(charlie_reg_task, return_exceptions=True)
        print("✔ TEST 7 PASSED: Background registration loop functions correctly.")

        # --------------------------------------------------------------------
        # TEST 8: Background Discovery Loop & Multiple Peers
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("👥 TEST 8: Background Discovery Loop & Multiple Peers")
        print("----------------------------------------------------------------------")
        # Charlie is already registered. Now we start discovery loop on a new PeerTable.
        multi_table = PeerTable()
        discovery_task = asyncio.create_task(
            rdv_conn.discovery_loop(
                namespace="TEST_NS",
                peer_table=multi_table,
                interval=2.0
            )
        )
        print("Background discovery loop started with 2.0s interval.")
        await asyncio.sleep(0.5)

        # Register multiple users dynamically
        print("Registering user1, user2, user3...")
        await rdv_conn.request({"type": "REGISTER", "namespace": "TEST_NS", "name": "user1", "port": 6001, "ttl": 30})
        await rdv_conn.request({"type": "REGISTER", "namespace": "TEST_NS", "name": "user2", "port": 6002, "ttl": 30})
        await rdv_conn.request({"type": "REGISTER", "namespace": "TEST_NS", "name": "user3", "port": 6003, "ttl": 30})
        
        print("Waiting 2.5 seconds for discovery loop to sync...")
        await asyncio.sleep(2.5)
        
        # Check PeerTable contains discovered users
        user_ids = [p.peer_id for p in multi_table.get_all_peers()]
        print(f"Peers in table: {user_ids}")
        assert "user1@TEST_NS" in user_ids, "user1 should be in table"
        assert "user2@TEST_NS" in user_ids, "user2 should be in table"
        assert "user3@TEST_NS" in user_ids, "user3 should be in table"
        print("✔ user1, user2, and user3 auto-discovered and synchronized in PeerTable.")
        
        # Register user4
        print("Registering user4...")
        await rdv_conn.request({"type": "REGISTER", "namespace": "TEST_NS", "name": "user4", "port": 6004, "ttl": 30})
        
        print("Waiting 2.5 seconds for next discovery sync...")
        await asyncio.sleep(2.5)
        
        user_ids = [p.peer_id for p in multi_table.get_all_peers()]
        print(f"Peers in table: {user_ids}")
        assert "user4@TEST_NS" in user_ids, "user4 should be in table"
        print("✔ user4 dynamically discovered and synchronized in PeerTable.")

        # Cleanup background discovery task
        discovery_task.cancel()
        await asyncio.gather(discovery_task, return_exceptions=True)
        
        # Clean up registered peers
        print("Cleaning up registered peers...")
        await rdv_conn.request({"type": "UNREGISTER", "namespace": "TEST_NS", "name": "charlie", "port": 7001})
        await rdv_conn.request({"type": "UNREGISTER", "namespace": "TEST_NS", "name": "user1", "port": 6001})
        await rdv_conn.request({"type": "UNREGISTER", "namespace": "TEST_NS", "name": "user2", "port": 6002})
        await rdv_conn.request({"type": "UNREGISTER", "namespace": "TEST_NS", "name": "user3", "port": 6003})
        await rdv_conn.request({"type": "UNREGISTER", "namespace": "TEST_NS", "name": "user4", "port": 6004})
        
        print("✔ TEST 8 PASSED: Background discovery loop and multiple peer synchronization verified.")

        # --------------------------------------------------------------------
        # TEST 9: Multi-User Concurrent P2P Connections & Keep-Alives
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("👥 TEST 9: Multi-User Concurrent P2P Connections & Keep-Alives")
        print("----------------------------------------------------------------------")
        
        # Initialize peer tables
        table_nodeA = PeerTable()
        table_nodeB = PeerTable()
        table_nodeC = PeerTable()
        
        # Pre-populate peer tables so they know how to connect to each other
        table_nodeA.peers["nodeB@TEST_NS"] = PeerEntry(name="nodeB", namespace="TEST_NS", ip="127.0.0.1", port=5011, ttl=300)
        table_nodeA.peers["nodeC@TEST_NS"] = PeerEntry(name="nodeC", namespace="TEST_NS", ip="127.0.0.1", port=5012, ttl=300)
        
        table_nodeB.peers["nodeA@TEST_NS"] = PeerEntry(name="nodeA", namespace="TEST_NS", ip="127.0.0.1", port=5010, ttl=300)
        table_nodeB.peers["nodeC@TEST_NS"] = PeerEntry(name="nodeC", namespace="TEST_NS", ip="127.0.0.1", port=5012, ttl=300)
        
        table_nodeC.peers["nodeA@TEST_NS"] = PeerEntry(name="nodeA", namespace="TEST_NS", ip="127.0.0.1", port=5010, ttl=300)
        table_nodeC.peers["nodeB@TEST_NS"] = PeerEntry(name="nodeB", namespace="TEST_NS", ip="127.0.0.1", port=5011, ttl=300)
        
        # Start servers
        nodeA_server = PeerServer(peer_id="nodeA@TEST_NS", port=5010, peer_table=table_nodeA)
        nodeB_server = PeerServer(peer_id="nodeB@TEST_NS", port=5011, peer_table=table_nodeB)
        nodeC_server = PeerServer(peer_id="nodeC@TEST_NS", port=5012, peer_table=table_nodeC)
        
        nodeA_server_task = asyncio.create_task(nodeA_server.start())
        nodeB_server_task = asyncio.create_task(nodeB_server.start())
        nodeC_server_task = asyncio.create_task(nodeC_server.start())
        
        await asyncio.sleep(0.5)
        print("Servers for nodeA (5010), nodeB (5011), and nodeC (5012) started.")
        
        # Establish Connections:
        # nodeA -> nodeB
        print("nodeA connecting to nodeB...")
        conn_A_to_B = PeerConnection(peer_id="nodeA@TEST_NS", peer_table=table_nodeA)
        conn_A_to_B.keepalive_interval = 2
        await conn_A_to_B.connect("127.0.0.1", 5011)
        listen_A_to_B = asyncio.create_task(conn_A_to_B.listen())
        
        # nodeA -> nodeC
        print("nodeA connecting to nodeC...")
        conn_A_to_C = PeerConnection(peer_id="nodeA@TEST_NS", peer_table=table_nodeA)
        conn_A_to_C.keepalive_interval = 2
        await conn_A_to_C.connect("127.0.0.1", 5012)
        listen_A_to_C = asyncio.create_task(conn_A_to_C.listen())
        
        # nodeB -> nodeC
        print("nodeB connecting to nodeC...")
        conn_B_to_C = PeerConnection(peer_id="nodeB@TEST_NS", peer_table=table_nodeB)
        conn_B_to_C.keepalive_interval = 2
        await conn_B_to_C.connect("127.0.0.1", 5012)
        listen_B_to_C = asyncio.create_task(conn_B_to_C.listen())
        
        # Wait a moment for handshakes and initial keepalives to settle
        print("Waiting 5.0 seconds for concurrent keep-alives and RTT calculations...")
        await asyncio.sleep(5.0)
        
        # Verify connection states in tables
        assert table_nodeA.get_peer("nodeB@TEST_NS").status == "CONNECTED", "nodeB should be CONNECTED in nodeA's table"
        assert table_nodeA.get_peer("nodeC@TEST_NS").status == "CONNECTED", "nodeC should be CONNECTED in nodeA's table"
        
        assert table_nodeB.get_peer("nodeA@TEST_NS").status == "CONNECTED", "nodeA should be CONNECTED in nodeB's table"
        assert table_nodeB.get_peer("nodeC@TEST_NS").status == "CONNECTED", "nodeC should be CONNECTED in nodeB's table"
        
        assert table_nodeC.get_peer("nodeA@TEST_NS").status == "CONNECTED", "nodeA should be CONNECTED in nodeC's table"
        assert table_nodeC.get_peer("nodeB@TEST_NS").status == "CONNECTED", "nodeB should be CONNECTED in nodeC's table"
        
        # Verify RTTs are computed and saved
        assert len(table_nodeA.get_peer("nodeB@TEST_NS").rtt_history) > 0, "nodeA should have measured RTT to nodeB"
        assert len(table_nodeA.get_peer("nodeC@TEST_NS").rtt_history) > 0, "nodeA should have measured RTT to nodeC"
        assert len(table_nodeB.get_peer("nodeC@TEST_NS").rtt_history) > 0, "nodeB should have measured RTT to nodeC"
        
        print(f"✔ Connection verification passed. nodeA average RTTs - nodeB: {table_nodeA.get_peer('nodeB@TEST_NS').average_rtt:.2f}ms, nodeC: {table_nodeA.get_peer('nodeC@TEST_NS').average_rtt:.2f}ms")
        
        # Simulate unexpected disconnect of nodeC by cancelling its server
        print("Simulating unexpected disconnect of nodeC...")
        nodeC_server_task.cancel()
        try:
            await nodeC_server_task
        except asyncio.CancelledError:
            pass
        
        print("nodeC stopped. Waiting for nodeA and nodeB to detect the connection drop...")
        await asyncio.sleep(1.5)
        
        # Verify tables reflect nodeC's disconnect
        assert table_nodeA.get_peer("nodeC@TEST_NS").status == "DISCONNECTED", "nodeC should be DISCONNECTED in nodeA's table"
        assert table_nodeB.get_peer("nodeC@TEST_NS").status == "DISCONNECTED", "nodeC should be DISCONNECTED in nodeB's table"
        print("✔ Disconnect detection passed. nodeA and nodeB updated nodeC's status to DISCONNECTED.")
        
        # Clean up remaining servers and active connections
        print("Cleaning up Test 9 servers and connections...")
        nodeA_server_task.cancel()
        nodeB_server_task.cancel()
        
        await asyncio.gather(nodeA_server_task, nodeB_server_task, return_exceptions=True)
        
        await conn_A_to_B.close()
        await conn_A_to_C.close()
        await conn_B_to_C.close()
        
        try:
            await listen_A_to_B
        except:
            pass
        try:
            await listen_A_to_C
        except:
            pass
        try:
            await listen_B_to_C
        except:
            pass
            
        print("✔ TEST 9 PASSED: Multi-user concurrent P2P connections, keep-alives and table updates verified successfully.")
        
        # --------------------------------------------------------------------
        # TEST 10: P2P Messaging (SEND, ACK, PUB)
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("💬 TEST 10: P2P Messaging (SEND, ACK, PUB)")
        print("----------------------------------------------------------------------")
        
        # Initialize peer tables
        table_sender = PeerTable()
        table_receiver = PeerTable()
        
        # Pre-populate peer tables so they know how to connect/recognize
        table_sender.peers["receiver@TEST_NS"] = PeerEntry(name="receiver", namespace="TEST_NS", ip="127.0.0.1", port=5021, ttl=300)
        table_receiver.peers["sender@TEST_NS"] = PeerEntry(name="sender", namespace="TEST_NS", ip="127.0.0.1", port=5020, ttl=300)
        
        # Start receiver server
        receiver_server = PeerServer(peer_id="receiver@TEST_NS", port=5021, peer_table=table_receiver)
        msg_server_task = asyncio.create_task(receiver_server.start())
        await asyncio.sleep(0.5)
        print("Receiver P2P Server started listening on port 5021.")
        
        # Create Sender's connection to Receiver
        msg_conn = PeerConnection(peer_id="sender@TEST_NS", peer_table=table_sender)
        msg_conn.keepalive_interval = 5
        
        print("Sender connecting to Receiver...")
        await msg_conn.connect("127.0.0.1", 5021)
        msg_listen_task = asyncio.create_task(msg_conn.listen())
        await asyncio.sleep(0.2)
        
        # 10.1: Send unicast SEND without require_ack
        print("Sending unicast SEND message (require_ack=False)...")
        await msg_conn.send_message(
            content="Hello receiver, this is a standard unicast message!",
            msg_id="msg_no_ack_123",
            require_ack=False
        )
        await asyncio.sleep(0.2)
        assert len(msg_conn._pending_acks) == 0, "No pending ACKs should exist"
        print("✔ Unicast SEND (no ACK) sent successfully.")

        # 10.2: Send unicast SEND with require_ack=True (Successful ACK)
        print("Sending unicast SEND message (require_ack=True)...")
        start_time = time.time()
        await msg_conn.send_message(
            content="Hello receiver, please acknowledge this message!",
            msg_id="msg_with_ack_456",
            require_ack=True
        )
        elapsed = time.time() - start_time
        print(f"Message sent and ACK received in {elapsed:.4f}s")
        assert elapsed < 4.0, "Should have received ACK immediately, not timed out"
        assert "msg_with_ack_456" not in msg_conn._pending_acks, "Pending ACK should be cleared"
        print("✔ Unicast SEND with require_ack=True completed successfully.")

        # 10.3: Send unicast SEND with require_ack=True, but simulate ACK timeout
        print("Simulating ACK timeout scenario...")
        msg_conn.ack_timeout = 0.5  # Fast timeout for testing
        
        # Monkey-patch the receiver server's active writer to drop the ACK response
        # This will simulate a scenario where the receiver gets the message but the ACK is lost or not sent
        ack_dropped = False
        if receiver_server.active_connections:
            writer_to_patch = list(receiver_server.active_connections)[0]
            original_write = writer_to_patch.write
            
            def dummy_write(data):
                nonlocal ack_dropped
                if b'"type": "ACK"' in data:
                    ack_dropped = True
                    print("[Test Sandbox] Dropping ACK response for timeout test simulation.")
                    return  # Drop the packet
                original_write(data)
                
            writer_to_patch.write = dummy_write
            
        start_time = time.time()
        await msg_conn.send_message(
            content="Hello receiver, this message should timeout waiting for ACK!",
            msg_id="msg_timeout_789",
            require_ack=True
        )
        elapsed = time.time() - start_time
        print(f"Message timeout test finished in {elapsed:.4f}s")
        assert elapsed >= 0.5, "Should have waited for at least 0.5s before timing out"
        assert ack_dropped, "We should have successfully intercepted and dropped the ACK"
        assert "msg_timeout_789" not in msg_conn._pending_acks, "Pending ACK should be cleaned up after timeout"
        print("✔ Unicast SEND ACK timeout handled correctly.")

        # 10.4: Publish PUB messages
        print("Sending PUB broadcast to namespace scope '#namespace'...")
        await msg_conn.publish(
            content="This is a broadcast to all namespace peers!",
            msg_id="pub_ns_999",
            scope="#namespace"
        )
        
        print("Sending PUB broadcast to global scope '*'...")
        await msg_conn.publish(
            content="This is a global broadcast!",
            msg_id="pub_global_888",
            scope="*"
        )
        await asyncio.sleep(0.2)
        print("✔ PUB broadcast messages sent successfully to single receiver.")
        
        # 10.5: Publish PUB messages with Fan-Out to multiple users
        print("\n--- 10.5: Testing Multi-User PUB Broadcast (Fan-Out) ---")
        # Start receiver 2 (same namespace)
        table_receiver2 = PeerTable()
        table_receiver2.peers["sender@TEST_NS"] = PeerEntry(name="sender", namespace="TEST_NS", ip="127.0.0.1", port=5020, ttl=300)
        receiver2_server = PeerServer(peer_id="receiver2@TEST_NS", port=5022, peer_table=table_receiver2)
        rec2_task = asyncio.create_task(receiver2_server.start())
        
        # Start receiver 3 (different namespace)
        table_receiver3 = PeerTable()
        table_receiver3.peers["sender@TEST_NS"] = PeerEntry(name="sender", namespace="TEST_NS", ip="127.0.0.1", port=5020, ttl=300)
        receiver3_server = PeerServer(peer_id="receiver3@OTHER_NS", port=5023, peer_table=table_receiver3)
        rec3_task = asyncio.create_task(receiver3_server.start())
        
        await asyncio.sleep(0.5)
        
        # Add to sender's table
        table_sender.peers["receiver2@TEST_NS"] = PeerEntry(name="receiver2", namespace="TEST_NS", ip="127.0.0.1", port=5022, ttl=300)
        table_sender.peers["receiver3@OTHER_NS"] = PeerEntry(name="receiver3", namespace="OTHER_NS", ip="127.0.0.1", port=5023, ttl=300)
        
        # Connect to them
        msg_conn2 = PeerConnection(peer_id="sender@TEST_NS", peer_table=table_sender)
        await msg_conn2.connect("127.0.0.1", 5022)
        msg_listen_task2 = asyncio.create_task(msg_conn2.listen())
        
        msg_conn3 = PeerConnection(peer_id="sender@TEST_NS", peer_table=table_sender)
        await msg_conn3.connect("127.0.0.1", 5023)
        msg_listen_task3 = asyncio.create_task(msg_conn3.listen())
        
        await asyncio.sleep(0.5)
        
        active_conns = [msg_conn, msg_conn2, msg_conn3]
        
        print("Sender broadcasting to '#namespace'...")
        fanout_msg_id = "pub_fanout_namespace"
        sender_namespace = "TEST_NS"
        for conn in active_conns:
            remote_peer = table_sender.get_peer(conn.remote_peer_id)
            if remote_peer and remote_peer.namespace == sender_namespace:
                await conn.publish(content="Hello TEST_NS group!", msg_id=fanout_msg_id, scope="#namespace")
        
        print("Sender broadcasting to '*' (Global)...")
        fanout_global_id = "pub_fanout_global"
        for conn in active_conns:
            await conn.publish(content="Hello everyone!", msg_id=fanout_global_id, scope="*")
            
        await asyncio.sleep(0.5)
        
        # Verify
        # receiver1 (TEST_NS) should have both
        r1_msgs = [m["msg_id"] for m in receiver_server.received_messages if m.get("type") == "PUB"]
        assert fanout_msg_id in r1_msgs, "Receiver1 should get namespace broadcast"
        assert fanout_global_id in r1_msgs, "Receiver1 should get global broadcast"
        
        # receiver2 (TEST_NS) should have both
        r2_msgs = [m["msg_id"] for m in receiver2_server.received_messages if m.get("type") == "PUB"]
        assert fanout_msg_id in r2_msgs, "Receiver2 should get namespace broadcast"
        assert fanout_global_id in r2_msgs, "Receiver2 should get global broadcast"
        
        # receiver3 (OTHER_NS) should ONLY have global
        r3_msgs = [m["msg_id"] for m in receiver3_server.received_messages if m.get("type") == "PUB"]
        assert fanout_msg_id not in r3_msgs, "Receiver3 should NOT get namespace broadcast"
        assert fanout_global_id in r3_msgs, "Receiver3 should get global broadcast"
        
        print("✔ Multi-User PUB Broadcast (Fan-Out) verified successfully!")

        # Clean up messaging connections/tasks
        print("Cleaning up Test 10 connections...")
        for c in active_conns:
            await c.close()
        for t in [msg_listen_task, msg_listen_task2, msg_listen_task3]:
            try:
                await t
            except:
                pass
        for st in [msg_server_task, rec2_task, rec3_task]:
            st.cancel()
            try:
                await st
            except asyncio.CancelledError:
                pass
            
        print("✔ TEST 10 PASSED: P2P Messaging features (SEND, require_ack, ACK timeout, PUB) verified successfully.")

        # --------------------------------------------------------------------
        # TEST 11: PeerTable Advanced Features (Reconnect, Stale, Remove)
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("📊 TEST 11: PeerTable Advanced Features")
        print("----------------------------------------------------------------------")
        adv_table = PeerTable(max_reconnect_attempts=3, initial_backoff_sec=0.1)
        
        # Manually add a peer
        adv_entry = PeerEntry(name="dave", namespace="TEST_NS", ip="127.0.0.1", port=5050, ttl=300)
        adv_table.peers[adv_entry.peer_id] = adv_entry
        
        # Test mark_connected and get_connected_peers
        adv_table.mark_connected(adv_entry.peer_id)
        connected = adv_table.get_connected_peers()
        assert len(connected) == 1, "Should have 1 connected peer"
        assert connected[0].peer_id == "dave@TEST_NS", "Connected peer should be dave"
        
        # Test mark_failed_attempt and backoff logic
        adv_table.mark_failed_attempt(adv_entry.peer_id)
        assert adv_entry.status == "RECONNECTING", "Status should be RECONNECTING after 1 fail"
        assert adv_entry.reconnect_attempts == 1, "Should have 1 reconnect attempt"
        
        # Initially, next_attempt_allowed_at > now, so it shouldn't be ready
        to_reconnect = adv_table.get_peers_to_reconnect()
        assert len(to_reconnect) == 0, "Should not be ready to reconnect immediately"
        
        # Wait for backoff (initial_backoff_sec * (2^0) = 0.1s)
        await asyncio.sleep(0.15)
        to_reconnect = adv_table.get_peers_to_reconnect()
        assert len(to_reconnect) == 1, "Should be ready to reconnect after backoff"
        
        # Fail until STALE
        adv_table.mark_failed_attempt(adv_entry.peer_id) # attempt 2
        adv_table.mark_failed_attempt(adv_entry.peer_id) # attempt 3 -> should trigger STALE since max_reconnect_attempts=3
        assert adv_entry.status == "STALE", "Status should be STALE after max attempts"
        
        # Test mark_stale manually
        adv_entry.status = "CONNECTED"
        adv_table.mark_stale(adv_entry.peer_id)
        assert adv_entry.status == "STALE", "Manual mark_stale failed"
        
        # Test remove_peer
        adv_table.remove_peer(adv_entry.peer_id)
        assert len(adv_table.get_all_peers()) == 0, "Table should be empty after remove_peer"
        print("✔ TEST 11 PASSED: PeerTable advanced features verified.")

        # --------------------------------------------------------------------
        # TEST 12: Config Loading
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("⚙️ TEST 12: Config Loading")
        print("----------------------------------------------------------------------")
        from config import Config
        cfg = Config() # Loads from default config.json
        assert cfg.app_name == "pyp2p-chat", "Config app_name should match default"
        assert cfg.max_reconnect_attempts > 0, "Config max_reconnect_attempts should be loaded"
        assert type(cfg.features) == list, "Config features should be a list"
        print("✔ TEST 12 PASSED: Config loading verified.")

        # --------------------------------------------------------------------
        # TEST 13: Main application clean shutdown
        # --------------------------------------------------------------------
        print("\n----------------------------------------------------------------------")
        print("🚀 TEST 13: Main Entrypoint Clean Shutdown")
        print("----------------------------------------------------------------------")
        import signal
        main_script = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main.py'))
        main_proc = subprocess.Popen(
            [sys.executable, main_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("Waiting for main.py to initialize and register...")
        await asyncio.sleep(2.0)
        
        print("Sending SIGINT to main.py to simulate graceful shutdown...")
        main_proc.send_signal(signal.SIGINT)
        
        stdout, stderr = main_proc.communicate(timeout=5)
        
        assert "Remoção de registro (UNREGISTER) concluída com sucesso" in stdout, "Main should perform clean UNREGISTER on shutdown"
        assert main_proc.returncode == 0, "Main should exit cleanly"
        print("✔ TEST 13 PASSED: main.py clean shutdown and UNREGISTER verified.")

    except AssertionError as ae:
        print(f"\n❌ Assertion Failed: {ae}")
        success = False
    except Exception as e:
        print(f"\n❌ Unexpected Exception: {e}")
        success = False
    finally:
        # Clean up any remaining tasks
        for conn in [bob_conn, conn_A_to_B, conn_A_to_C, conn_B_to_C, msg_conn, msg_conn2, msg_conn3]:
            if conn:
                try:
                    await conn.close()
                except:
                    pass
        for task in [alice_server_task, nodeA_server_task, nodeB_server_task, nodeC_server_task, msg_server_task, rec2_task, rec3_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except:
                    pass
            
        # Stop local Rendezvous server
        rdv_proc.terminate()
        rdv_proc.wait()
        print("\n[RDV] Rendezvous Server stopped.")

    print("\n======================================================================")
    if success:
        print("🎉 ALL TESTS PASSED SUCCESSFULLY! EVERYTHING IS WORKING PERFECTLY.")
    else:
        print("❌ TEST SUITE FAILED. PLEASE REVIEW LOGS.")
    print("======================================================================")

if __name__ == "__main__":
    asyncio.run(run_test_suite())
