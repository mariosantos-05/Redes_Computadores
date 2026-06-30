import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from peer_table import PeerTable, PeerEntry
from reconnection_manager import reconnection_loop

async def main():
    print("--- Running Test 7: P2P Reconnection Logic & Backoff ---")
    
    # 1. Setup peer table with max_reconnect_attempts = 3
    table = PeerTable(max_reconnect_attempts=3)
    table.initial_backoff_sec = 0.5  # short backoff for faster test execution

    # 2. Add an offline peer to table
    peer_id = "offline_peer@TEST_NS"
    peer = PeerEntry(name="offline_peer", namespace="TEST_NS", ip="127.0.0.1", port=9999, ttl=300)
    # Put it in RECONNECTING state and allow immediate connection
    peer.status = "RECONNECTING"
    peer.next_attempt_allowed_at = time.time() - 10
    table.peers[peer_id] = peer

    outbound_connections = {}

    # 3. Start reconnection loop task with a small sleep interval
    loop_task = asyncio.create_task(
        reconnection_loop(
            peer_id="local@TEST_NS",
            peer_table=table,
            outbound_connections=outbound_connections,
            interval=0.1
        )
    )

    # 4. Wait for the first attempt to trigger and fail
    print("Waiting for first reconnection attempt to fail...")
    await asyncio.sleep(0.5)

    print(f"Attempts: {peer.reconnect_attempts}, Status: {peer.status}")
    assert peer.reconnect_attempts == 1, "Should have attempted once"
    assert peer.status == "RECONNECTING", "Status should be RECONNECTING"

    # Fast forward allowed time to trigger second attempt
    print("Fast-forwarding backoff time for second attempt...")
    peer.next_attempt_allowed_at = time.time() - 10
    await asyncio.sleep(0.5)

    print(f"Attempts: {peer.reconnect_attempts}, Status: {peer.status}")
    assert peer.reconnect_attempts == 2, "Should have attempted twice"
    assert peer.status == "RECONNECTING", "Status should still be RECONNECTING"

    # Fast forward allowed time to trigger third attempt (which reaches max limit of 3)
    print("Fast-forwarding backoff time for third attempt (max)...")
    peer.next_attempt_allowed_at = time.time() - 10
    await asyncio.sleep(0.5)

    print(f"Attempts: {peer.reconnect_attempts}, Status: {peer.status}")
    assert peer.status == "STALE", "Status should be STALE after 3 failed attempts"
    print("Peer status successfully transitioned to STALE.")

    # Cleanup
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass

    print("Test 7 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
