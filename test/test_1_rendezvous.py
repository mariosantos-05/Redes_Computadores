import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rendezvous_connection import RendezvousConnection
from config import Config
from peer_table import PeerTable

async def main():
    print("--- Running Test 1: Rendezvous connection, registration, discovery and unregistration ---")
    config = Config()
    rdv_host = config.rdv_host
    rdv_port = config.rdv_port
    namespace = "TEST_NS"

    rdv_conn = RendezvousConnection(rdv_host, rdv_port)

    # 1. Test REGISTER
    print("Testing REGISTER...")
    reg_msg = {
        "type": "REGISTER",
        "namespace": namespace,
        "name": "test_peer_1",
        "port": 9001,
        "ttl": 15
    }
    response = await rdv_conn.request(reg_msg)
    print("REGISTER response:", response)
    assert response.get("status") == "OK", "Register status should be OK"

    # 2. Test DISCOVER
    print("Testing DISCOVER...")
    disc_msg = {
        "type": "DISCOVER",
        "namespace": namespace
    }
    response = await rdv_conn.request(disc_msg)
    print("DISCOVER response:", response)
    assert response.get("status") == "OK", "Discover status should be OK"
    peers = response.get("peers", [])
    found = any(p["name"] == "test_peer_1" and p["port"] == 9001 for p in peers)
    assert found, "Registered peer not found in discovery"

    # 3. Test registration loop
    print("Testing registration loop...")
    reg_loop_task = asyncio.create_task(
        rdv_conn.registration_loop(
            namespace=namespace,
            name="test_peer_1",
            port=9001,
            initial_ttl=5
        )
    )
    # Let it run for 6 seconds (exceeding initial ttl, so it should renew)
    await asyncio.sleep(6.0)
    # Verify it is still there and has a fresh TTL
    response = await rdv_conn.request(disc_msg)
    peers = response.get("peers", [])
    peer_entry = next((p for p in peers if p["name"] == "test_peer_1"), None)
    assert peer_entry is not None, "Peer should still be registered after TTL expiration due to renewal loop"
    # Cancel the registration loop
    reg_loop_task.cancel()
    try:
        await reg_loop_task
    except asyncio.CancelledError:
        pass

    # 4. Test discovery loop
    print("Testing discovery loop...")
    peer_table = PeerTable()
    disc_loop_task = asyncio.create_task(
        rdv_conn.discovery_loop(
            namespace=namespace,
            peer_table=peer_table,
            interval=2.0
        )
    )
    # Register another peer so the discovery loop picks it up
    await rdv_conn.request({
        "type": "REGISTER",
        "namespace": namespace,
        "name": "test_peer_2",
        "port": 9002,
        "ttl": 15
    })
    # Wait for discovery loop to run
    await asyncio.sleep(3.0)
    # Check if peer_table was updated
    assert peer_table.get_peer("test_peer_2@TEST_NS") is not None, "Discovery loop should have added test_peer_2 to peer table"
    
    disc_loop_task.cancel()
    try:
        await disc_loop_task
    except asyncio.CancelledError:
        pass

    # 5. Test UNREGISTER
    print("Testing UNREGISTER...")
    unreg_msg = {
        "type": "UNREGISTER",
        "namespace": namespace,
        "name": "test_peer_1",
        "port": 9001
    }
    response = await rdv_conn.request(unreg_msg)
    print("UNREGISTER response:", response)
    assert response.get("status") == "OK", "Unregister status should be OK"

    # Verify discovery after unregister
    response = await rdv_conn.request(disc_msg)
    peers = response.get("peers", [])
    found_1 = any(p["name"] == "test_peer_1" for p in peers)
    assert not found_1, "Unregistered peer 1 should not be found in discovery"

    # Cleanup peer 2
    await rdv_conn.request({
        "type": "UNREGISTER",
        "namespace": namespace,
        "name": "test_peer_2",
        "port": 9002
    })

    print("Test 1 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
