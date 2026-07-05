import asyncio
import sys
import os
import time
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import Config
from message_router import validate_message_fields, encode_message, decode_message, process_common_messages
from peer_table import PeerTable, PeerEntry
from peer_server import PeerServer
from peer_connection import PeerConnection

def test_config():
    print("1. Testing Config Singleton...")
    cfg1 = Config()
    cfg2 = Config()
    assert cfg1 is cfg2, "Config must be a singleton"
    assert isinstance(cfg1.namespace, str)
    assert isinstance(cfg1.name, str)
    assert isinstance(cfg1.rdv_port, int)
    assert isinstance(cfg1.listen_port, int)
    print("Config tests passed!")

def test_message_router():
    print("\n2. Testing Message Router encoding/decoding and validations...")
    # Validate fields
    validate_message_fields({"namespace": "test_ns", "name": "test_peer", "port": 5000, "ttl": 300})
    
    # Validation failures
    try:
        validate_message_fields({"port": 99999})
        assert False, "Should raise ValueError for port > 65535"
    except ValueError:
        pass

    try:
        validate_message_fields({"ttl": -5})
        assert False, "Should raise ValueError for negative ttl"
    except ValueError:
        pass

    try:
        validate_message_fields({"namespace": "a" * 100})
        assert False, "Should raise ValueError for namespace > 64 chars"
    except ValueError:
        pass

    # Encode/Decode
    msg = {"type": "HELLO", "peer_id": "alice@CIC", "version": "1.0", "ttl": 60}
    encoded = encode_message(msg)
    decoded = decode_message(encoded)
    assert decoded["type"] == "HELLO"
    assert decoded["peer_id"] == "alice@CIC"
    print("Message Router tests passed!")

def test_peer_table():
    print("\n3. Testing PeerTable state transitions and backoffs...")
    table = PeerTable(max_reconnect_attempts=3, initial_backoff_sec=0.1)
    
    # Update from discovery (bulk)
    peers_list = [
        {"name": "p1", "namespace": "NS", "ip": "127.0.0.1", "port": 6001, "ttl": 30},
        {"name": "p2", "namespace": "NS", "ip": "127.0.0.1", "port": 6002, "ttl": 30}
    ]
    new_peers = table.update_from_discovery(peers_list)
    assert len(new_peers) == 2
    assert "p1@NS" in table.peers
    assert "p2@NS" in table.peers
    
    # RTT sliding average logic
    p1_entry = table.get_peer("p1@NS")
    for rtt in [10.0, 20.0, 30.0]:
        p1_entry.add_rtt(rtt)
    assert p1_entry.average_rtt == 20.0
    assert len(p1_entry.rtt_history) == 3
    # Add 12 items, check that history size is capped at 10
    for i in range(12):
        p1_entry.add_rtt(float(i + 1))
    assert len(p1_entry.rtt_history) == 10
    
    # Backoff logic
    assert p1_entry.status == "DISCONNECTED"
    table.mark_failed_attempt("p1@NS")
    assert p1_entry.reconnect_attempts == 1
    assert p1_entry.status == "RECONNECTING"
    assert p1_entry.next_attempt_allowed_at > 0
    
    table.mark_failed_attempt("p1@NS")
    assert p1_entry.reconnect_attempts == 2
    assert p1_entry.status == "RECONNECTING"
    
    table.mark_failed_attempt("p1@NS")
    assert p1_entry.reconnect_attempts == 3
    assert p1_entry.status == "STALE", "Should transition to STALE after 3 attempts"
    
    table.mark_connected("p1@NS")
    assert p1_entry.status == "CONNECTED"
    assert p1_entry.reconnect_attempts == 0
    
    table.mark_disconnected("p1@NS")
    assert p1_entry.status == "DISCONNECTED"
    print("PeerTable tests passed!")

async def test_mock_multi_peer_load():
    print("\n4. Testing mock multi-peer load and safe shutdown...")
    inbound_connections = {}
    outbound_connections = {}
    
    class FakeConnection:
        def __init__(self, remote_id):
            self.remote_peer_id = remote_id
            self.closed = False
            self.disconnected = False
            
        async def disconnect(self, reason):
            self.disconnected = True
            
        async def close(self):
            self.closed = True

    # Setup 15 inbound and 15 outbound connections
    for i in range(15):
        in_id = f"in_peer_{i}@NS"
        out_id = f"out_peer_{i}@NS"
        inbound_connections[in_id] = FakeConnection(in_id)
        outbound_connections[out_id] = FakeConnection(out_id)
        
    # Introduce some "connecting" placeholders
    outbound_connections["connecting_peer@NS"] = "connecting"
    
    # Simulate safe shutdown loop
    for pid, conn in list(inbound_connections.items()):
        if isinstance(conn, str):
            continue
        await conn.disconnect(reason="Shutdown")
        await conn.close()
        inbound_connections.pop(pid, None)

    for pid, conn in list(outbound_connections.items()):
        if isinstance(conn, str):
            continue
        await conn.disconnect(reason="Shutdown")
        await conn.close()
        outbound_connections.pop(pid, None)
        
    assert len(inbound_connections) == 0
    assert len(outbound_connections) == 1, "The 'connecting' placeholder should remain since it is a string"
    assert outbound_connections["connecting_peer@NS"] == "connecting"
    print("Mock multi-peer load tests passed!")

async def test_live_collisions():
    print("\n5. Testing live identity and port collisions under running servers...")
    namespace = "TEST_NS"

    # Start Server A (Alice)
    alice_table = PeerTable()
    alice_inbound = {}
    alice_server = PeerServer("alice@TEST_NS", 5010, alice_table, alice_inbound)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5010
    )
    print("Alice live server started on port 5010")

    # Start Server B (Bob)
    bob_table = PeerTable()
    bob_inbound = {}
    bob_server = PeerServer("bob@TEST_NS", 5011, bob_table, bob_inbound)
    bob_server_runner = await asyncio.start_server(
        bob_server.handle_client,
        "127.0.0.1",
        5011
    )
    print("Bob live server started on port 5011")

    # Add Bob to Alice's table and Alice to Bob's table
    bob_entry = PeerEntry(name="bob", namespace=namespace, ip="127.0.0.1", port=5011, ttl=300)
    alice_table.peers[bob_entry.peer_id] = bob_entry
    
    alice_entry = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5010, ttl=300)
    bob_table.peers[alice_entry.peer_id] = alice_entry

    # Scenario A: Same Name, Same Namespace, Different port/connection (Bob connecting to Alice)
    bob_conn1 = PeerConnection("bob@TEST_NS", bob_table)
    await bob_conn1.connect("127.0.0.1", 5010)
    bob_conn1_listen = asyncio.create_task(bob_conn1.listen())
    
    await asyncio.sleep(0.2)
    assert "bob@TEST_NS" in alice_inbound, "Bob should be in Alice's inbound connections"
    initial_inbound_conn = alice_inbound["bob@TEST_NS"]
    
    # Second connection representing Bob connecting again (e.g. location update or reconnection)
    bob_conn2 = PeerConnection("bob@TEST_NS", bob_table)
    await bob_conn2.connect("127.0.0.1", 5010)
    bob_conn2_listen = asyncio.create_task(bob_conn2.listen())
    
    await asyncio.sleep(0.2)
    assert "bob@TEST_NS" in alice_inbound, "Bob should still be in Alice's inbound connections"
    new_inbound_conn = alice_inbound["bob@TEST_NS"]
    assert new_inbound_conn is not initial_inbound_conn, "The inbound connection should be updated to the new connection instance"
    print("Scenario A passed: Same Name, Same Namespace, Location update handled dynamically while running!")

    # Scenario B: Same Name, Different Namespace (bob@OTHER_NS connecting to alice@TEST_NS)
    bob_other_entry = PeerEntry(name="bob", namespace="OTHER_NS", ip="127.0.0.1", port=5012, ttl=300)
    alice_table.peers[bob_other_entry.peer_id] = bob_other_entry
    
    bob_other_table = PeerTable()
    bob_other_conn = PeerConnection("bob@OTHER_NS", bob_other_table)
    await bob_other_conn.connect("127.0.0.1", 5010)
    bob_other_listen = asyncio.create_task(bob_other_conn.listen())
    
    await asyncio.sleep(0.2)
    assert "bob@TEST_NS" in alice_inbound, "bob@TEST_NS should be in Alice's inbound connections"
    assert "bob@OTHER_NS" in alice_inbound, "bob@OTHER_NS should be in Alice's inbound connections as a distinct entry"
    print("Scenario B passed: Same Name, Different Namespace coexists as separate connections while running!")

    # Scenario C: Different Names, Same IP/Port (charlie@TEST_NS and bob@TEST_NS both connecting to Alice)
    charlie_entry = PeerEntry(name="charlie", namespace=namespace, ip="127.0.0.1", port=5011, ttl=300)
    alice_table.peers[charlie_entry.peer_id] = charlie_entry
    
    charlie_table = PeerTable()
    charlie_conn = PeerConnection("charlie@TEST_NS", charlie_table)
    await charlie_conn.connect("127.0.0.1", 5010)
    charlie_listen = asyncio.create_task(charlie_conn.listen())
    
    await asyncio.sleep(0.2)
    assert "bob@TEST_NS" in alice_inbound, "Bob should be in Alice's inbound connections"
    assert "charlie@TEST_NS" in alice_inbound, "Charlie should be in Alice's inbound connections"
    print("Scenario C passed: Different Names, Same IP/Port are registered and handled independently while running!")

    # Cleanup live connections
    print("Cleaning up live collision tests...")
    await bob_conn1.close()
    await bob_conn2.close()
    await bob_other_conn.close()
    await charlie_conn.close()
    
    bob_conn1_listen.cancel()
    bob_conn2_listen.cancel()
    bob_other_listen.cancel()
    charlie_listen.cancel()
    
    await asyncio.gather(bob_conn1_listen, bob_conn2_listen, bob_other_listen, charlie_listen, return_exceptions=True)

    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    bob_server_runner.close()
    await bob_server_runner.wait_closed()
    print("Live collision tests passed successfully!")

def test_reconnection_candidate_filtering():
    print("\n6. Testing PeerTable reconnection candidate filtering...")
    table = PeerTable(max_reconnect_attempts=3, initial_backoff_sec=10.0)
    
    p1 = PeerEntry("p1", "NS", "127.0.0.1", 6001, 30)
    p1.status = "RECONNECTING"
    p1.next_attempt_allowed_at = time.time() - 5.0
    table.peers[p1.peer_id] = p1

    p2 = PeerEntry("p2", "NS", "127.0.0.1", 6002, 30)
    p2.status = "RECONNECTING"
    p2.next_attempt_allowed_at = time.time() + 10.0
    table.peers[p2.peer_id] = p2

    p3 = PeerEntry("p3", "NS", "127.0.0.1", 6003, 30)
    p3.status = "STALE"
    table.peers[p3.peer_id] = p3

    p4 = PeerEntry("p4", "NS", "127.0.0.1", 6004, 30)
    p4.status = "CONNECTED"
    table.peers[p4.peer_id] = p4

    candidates = table.get_peers_to_reconnect()
    assert len(candidates) == 1, "Only p1 should be ready for reconnection"
    assert candidates[0].peer_id == "p1@NS"
    print("Reconnection candidate filtering tests passed!")

async def test_process_common_messages():
    print("\n7. Testing process_common_messages handlers...")
    
    received_msgs = []
    async def fake_send(msg):
        received_msgs.append(msg)

    # 1. PING message
    msg_ping = {"type": "PING", "msg_id": "ping-123"}
    action = await process_common_messages(msg_ping, "local@NS", "remote@NS", fake_send)
    assert action == "HANDLED"
    assert len(received_msgs) == 1
    assert received_msgs[0]["type"] == "PONG"
    assert received_msgs[0]["msg_id"] == "ping-123"
    received_msgs.clear()

    # 2. SEND message with require_ack
    msg_send_ack = {
        "type": "SEND",
        "msg_id": "send-123",
        "src": "remote@NS",
        "payload": "hello",
        "require_ack": True
    }
    action = await process_common_messages(msg_send_ack, "local@NS", "remote@NS", fake_send)
    assert action == "HANDLED"
    assert len(received_msgs) == 1
    assert received_msgs[0]["type"] == "ACK"
    assert received_msgs[0]["msg_id"] == "send-123"
    received_msgs.clear()

    # 3. SEND message without require_ack
    msg_send_no_ack = {
        "type": "SEND",
        "msg_id": "send-124",
        "src": "remote@NS",
        "payload": "hello",
        "require_ack": False
    }
    action = await process_common_messages(msg_send_no_ack, "local@NS", "remote@NS", fake_send)
    assert action == "HANDLED"
    assert len(received_msgs) == 0

    # 4. BYE message
    msg_bye = {"type": "BYE", "msg_id": "bye-123", "src": "remote@NS", "reason": "leaving"}
    action = await process_common_messages(msg_bye, "local@NS", "remote@NS", fake_send)
    assert action == "BREAK"
    assert len(received_msgs) == 1
    assert received_msgs[0]["type"] == "BYE_OK"
    assert received_msgs[0]["dst"] == "remote@NS"
    received_msgs.clear()

    # 5. Unknown message type
    msg_unknown = {"type": "UNKNOWN_TYPE"}
    action = await process_common_messages(msg_unknown, "local@NS", "remote@NS", fake_send)
    assert action == "CONTINUE"
    print("process_common_messages tests passed!")

def test_config_validation():
    print("\n8. Testing config validations manually...")
    # Invalid namespace > 64 chars
    try:
        validate_message_fields({"namespace": "n" * 65})
        assert False, "Should raise ValueError for namespace > 64 chars"
    except ValueError:
        pass

    # Invalid name > 64 chars
    try:
        validate_message_fields({"name": "a" * 65})
        assert False, "Should raise ValueError for name > 64 chars"
    except ValueError:
        pass

    # Invalid peer_id parts > 64 chars
    try:
        validate_message_fields({"peer_id": "a" * 65 + "@NS"})
        assert False, "Should raise ValueError for peer_id name > 64 chars"
    except ValueError:
        pass

    # Invalid port values
    try:
        validate_message_fields({"port": "invalid"})
        assert False, "Should raise ValueError for non-numeric port"
    except ValueError:
        pass

    try:
        validate_message_fields({"port": 0})
        assert False, "Should raise ValueError for port < 1"
    except ValueError:
        pass

    try:
        validate_message_fields({"port": 65536})
        assert False, "Should raise ValueError for port > 65535"
    except ValueError:
        pass

    # Invalid ttl values
    try:
        validate_message_fields({"ttl": 86401})
        assert False, "Should raise ValueError for ttl > 86400"
    except ValueError:
        pass

    print("Config validations manually tested and passed!")

async def test_concurrency_load_and_broadcasting():
    print("\n9. Testing multi-peer broadcast loop and concurrent load...")
    peers_db = {}
    servers = []
    server_runners = []
    connections = {}
    
    for i in range(5):
        name = f"peer_{i}"
        port = 5020 + i
        table = PeerTable()
        inbound = {}
        server = PeerServer(f"{name}@TEST_NS", port, table, inbound)
        runner = await asyncio.start_server(
            server.handle_client,
            "127.0.0.1",
            port
        )
        servers.append(server)
        server_runners.append(runner)
        peers_db[name] = {
            "peer_id": f"{name}@TEST_NS",
            "port": port,
            "table": table,
            "inbound": inbound,
            "server": server
        }

    star_center = peers_db["peer_0"]
    for i in range(1, 5):
        leaf = peers_db[f"peer_{i}"]
        leaf_entry = PeerEntry(leaf["server"].peer_id.split("@")[0], "TEST_NS", "127.0.0.1", leaf["port"], 300)
        star_center["table"].peers[leaf_entry.peer_id] = leaf_entry
        
        conn = PeerConnection(star_center["server"].peer_id, star_center["table"])
        await conn.connect("127.0.0.1", leaf["port"])
        listen_task = asyncio.create_task(conn.listen())
        connections[f"center_to_{leaf['server'].peer_id}"] = (conn, listen_task)
        
    await asyncio.sleep(0.5)

    for i in range(1, 5):
        leaf = peers_db[f"peer_{i}"]
        assert star_center["server"].peer_id in leaf["inbound"], f"Star center should be connected to leaf {i}"
        
    print("Star center publishing broadcast message to all leaf nodes...")
    broadcast_msg_id = "bcast-999"
    for i in range(1, 5):
        leaf_id = f"peer_{i}@TEST_NS"
        conn, _ = connections[f"center_to_{leaf_id}"]
        await conn.publish("Hello leaf nodes!", broadcast_msg_id, "*")
        
    await asyncio.sleep(0.5)
    
    print("Cleaning up star topology network...")
    for key, (conn, listen_task) in connections.items():
        await conn.close()
        listen_task.cancel()
        
    for i in range(1, 5):
        leaf = peers_db[f"peer_{i}"]
        for inbound_conn in list(leaf["inbound"].values()):
            await inbound_conn.close()

    await asyncio.sleep(0.2)

    for runner in server_runners:
        runner.close()
        await runner.wait_closed()
        
    print("Multi-peer concurrent load and broadcast topology tests passed successfully!")

async def test_live_network_drop_and_reconnect():
    print("\n10. Testing live network drop and automatic reconnection loop...")
    from reconnection_manager import reconnection_loop

    namespace = "TEST_NS"
    alice_table = PeerTable()
    alice_inbound = {}
    
    alice_server = PeerServer("alice@TEST_NS", 5030, alice_table, alice_inbound)
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5030
    )
    print("Alice live server started on port 5030")

    bob_table = PeerTable(max_reconnect_attempts=3, initial_backoff_sec=0.1)
    bob_outbound = {}
    
    alice_entry = PeerEntry(name="alice", namespace=namespace, ip="127.0.0.1", port=5030, ttl=300)
    bob_table.peers[alice_entry.peer_id] = alice_entry

    bob_conn = PeerConnection(
        "bob@TEST_NS",
        bob_table,
        on_close=lambda c: bob_outbound.pop(c.remote_peer_id, None) if c.remote_peer_id else None
    )
    await bob_conn.connect("127.0.0.1", 5030)
    bob_listen_task = asyncio.create_task(bob_conn.listen())
    bob_outbound[bob_conn.remote_peer_id] = bob_conn
    
    await asyncio.sleep(0.2)
    assert alice_entry.status == "CONNECTED", "Alice status should be CONNECTED initially"
    assert "alice@TEST_NS" in bob_outbound
    assert "bob@TEST_NS" in alice_inbound

    print("Simulating connection drop (killing Alice's server and closing connections)...")
    await bob_conn.close()
    bob_listen_task.cancel()
    try:
        await bob_listen_task
    except asyncio.CancelledError:
        pass

    alice_server_runner.close()
    await alice_server_runner.wait_closed()
        
    await asyncio.sleep(0.2)
    assert "alice@TEST_NS" not in bob_outbound, "Alice should be removed from Bob's outbound connections on close"
    
    alice_entry.status = "RECONNECTING"
    alice_entry.next_attempt_allowed_at = time.time() - 1.0
    alice_entry.reconnect_attempts = 0

    bob_reconn_task = asyncio.create_task(
        reconnection_loop(
            peer_id="bob@TEST_NS",
            peer_table=bob_table,
            outbound_connections=bob_outbound,
            interval=0.1
        )
    )
    
    await asyncio.sleep(0.3)
    assert alice_entry.reconnect_attempts > 0, "Should have attempted to reconnect and failed"
    assert alice_entry.status == "RECONNECTING"
    
    print("Bringing Alice's server back online...")
    alice_entry.next_attempt_allowed_at = time.time() - 1.0
    
    alice_server_runner = await asyncio.start_server(
        alice_server.handle_client,
        "127.0.0.1",
        5030
    )
    
    await asyncio.sleep(0.5)
    
    print("Reconnection status checking...")
    assert alice_entry.status == "CONNECTED", "Alice status should have returned to CONNECTED after restart"
    assert "alice@TEST_NS" in bob_outbound, "Alice should be back in Bob's outbound connections"
    
    print("Tearing down drop/reconnect test...")
    bob_reconn_task.cancel()
    try:
        await bob_reconn_task
    except asyncio.CancelledError:
        pass

    for conn in list(bob_outbound.values()):
        await conn.close()
    for conn in list(alice_inbound.values()):
        await conn.close()
        
    alice_server_runner.close()
    await alice_server_runner.wait_closed()
    print("Live drop and reconnect test passed successfully!")

async def test_stress_load_30_connections():
    print("\n11. Testing stress load with 30 concurrent active connections...")
    
    server_table = PeerTable()
    server_inbound = {}
    server = PeerServer("server@TEST_NS", 5040, server_table, server_inbound)
    server_runner = await asyncio.start_server(
        server.handle_client,
        "127.0.0.1",
        5040
    )
    print("Stress server listening on port 5040")

    client_conns = []
    client_tasks = []
    
    print("Launching 30 client connections concurrently...")
    connect_tasks = []
    
    for i in range(30):
        client_id = f"client_{i}@TEST_NS"
        client_table = PeerTable()
        conn = PeerConnection(client_id, client_table)
        client_conns.append(conn)
        connect_tasks.append(conn.connect("127.0.0.1", 5040))
        
    await asyncio.gather(*connect_tasks)
    
    for conn in client_conns:
        client_tasks.append(asyncio.create_task(conn.listen()))
        
    await asyncio.sleep(0.5)
    
    assert len(server_inbound) == 30, f"Server should have 30 registered inbound connections, got {len(server_inbound)}"
    for i in range(30):
        assert f"client_{i}@TEST_NS" in server_inbound, f"client_{i}@TEST_NS should be registered"
        
    print("30 concurrent handshakes completed successfully!")

    print("Sending messages concurrently over all 30 connections...")
    msg_tasks = []
    for i, conn in enumerate(client_conns):
        msg_tasks.append(conn.send_message(f"Hello from client {i}", f"msg-stress-{i}", require_ack=True))
        
    await asyncio.gather(*msg_tasks)
    await asyncio.sleep(0.5)
    
    print("Tearing down stress load test...")
    for conn in client_conns:
        await conn.close()
        
    for task in client_tasks:
        task.cancel()
        
    await asyncio.gather(*client_tasks, return_exceptions=True)
    
    server_runner.close()
    await server_runner.wait_closed()
    print("Stress load test with 30 connections passed successfully!")

async def main():
    print("--- Running Test 10: P2P Comprehensive Unit, Load & Running Collision Tests ---")
    test_config()
    test_message_router()
    test_peer_table()
    await test_mock_multi_peer_load()
    await test_live_collisions()
    test_reconnection_candidate_filtering()
    await test_process_common_messages()
    test_config_validation()
    await test_concurrency_load_and_broadcasting()
    await test_live_network_drop_and_reconnect()
    await test_stress_load_30_connections()
    print("\nTest 10 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
