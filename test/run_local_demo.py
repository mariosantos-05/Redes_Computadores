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

async def main():
    print("======================================================================")
    print("🚀 STARTING LOCAL END-TO-END P2P & RENDEZVOUS KEEP-ALIVE DEMO")
    print("======================================================================")

    # 1. Start the local Rendezvous Server in a subprocess
    rdv_port = 8080
    rdv_host = "127.0.0.1"
    
    rdv_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pyp2p-rdv", "src", "rendezvous", "main.py"))
    if not os.path.exists(rdv_script):
        print(f"❌ Error: Rendezvous script not found at {rdv_script}")
        return

    print("Step 1: Starting Rendezvous Server...")
    rdv_proc = subprocess.Popen(
        [sys.executable, rdv_script, "--host", rdv_host, "--port", str(rdv_port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # Wait for the RDV server to spin up
    await asyncio.sleep(2)
    print("✔ Rendezvous Server started successfully!")

    # 2. Setup Rendezvous connection manager for Peer A and Peer B
    rdv_conn = RendezvousConnection(rdv_host, rdv_port)

    # 3. Setup Peer A (Alice)
    print("\nStep 2: Starting Peer A (Alice)...")
    alice_table = PeerTable()
    alice_port = 5002
    alice_id = "alice@CIC"
    alice_server = PeerServer(peer_id=alice_id, port=alice_port, peer_table=alice_table)
    
    # Run Alice's server in the background
    alice_server_task = asyncio.create_task(alice_server.start())
    
    # Register Alice on Rendezvous
    reg_alice = {
        "type": "REGISTER",
        "namespace": "CIC",
        "name": "alice",
        "port": alice_port,
        "ttl": 60
    }
    await rdv_conn.request(reg_alice)
    print("✔ Peer A (Alice) registered on Rendezvous.")

    # 4. Setup Peer B (Bob)
    print("\nStep 3: Starting Peer B (Bob)...")
    bob_table = PeerTable()
    bob_port = 5003
    bob_id = "bob@CIC"
    bob_server = PeerServer(peer_id=bob_id, port=bob_port, peer_table=bob_table)
    
    # Run Bob's server in the background
    bob_server_task = asyncio.create_task(bob_server.start())
    
    # Register Bob on Rendezvous
    reg_bob = {
        "type": "REGISTER",
        "namespace": "CIC",
        "name": "bob",
        "port": bob_port,
        "ttl": 60
    }
    await rdv_conn.request(reg_bob)
    print("✔ Peer B (Bob) registered on Rendezvous.")

    # 5. Bob queries DISCOVER to find Alice
    print("\nStep 4: Peer B (Bob) executing DISCOVER for namespace 'CIC'...")
    discover_msg = {
        "type": "DISCOVER",
        "namespace": "CIC"
    }
    response = await rdv_conn.request(discover_msg)
    
    if response.get("status") == "OK":
        peers = response.get("peers", [])
        print(f"✔ DISCOVER Success: Found {len(peers)} peers.")
        # Sincroniza a tabela de peers do Bob
        bob_table.update_from_discovery(peers)
    else:
        print("❌ DISCOVER Failed:", response)
        return

    # 6. Bob initiates TCP connection to Alice
    alice_entry = bob_table.get_peer(alice_id)
    if not alice_entry:
        print("❌ Error: Alice not found in Bob's discovered peer list.")
        return
        
    print(f"\nStep 5: Peer B (Bob) connecting to Peer A (Alice) at {alice_entry.ip}:{alice_entry.port}...")
    
    bob_conn = PeerConnection(peer_id=bob_id, peer_table=bob_table)
    # Set a fast keepalive interval (3 seconds) for the demo
    bob_conn.keepalive_interval = 3
    
    await bob_conn.connect(alice_entry.ip, alice_entry.port)
    
    # Start listening to Alice
    bob_listen_task = asyncio.create_task(bob_conn.listen())
    
    print(f"Status of Alice in Bob's table: {alice_entry.status}")
    
    # 7. Let keep-alives run and exchange PING/PONG for 10 seconds
    print("\nStep 6: Keeping connection alive for 10 seconds to observe PING/PONG & RTT...")
    await asyncio.sleep(10)

    # Print RTT history
    print("\n======================================================================")
    print(f"📈 RESULTS FOR {alice_id}:")
    print(f"RTT History: {alice_entry.rtt_history}")
    print(f"Average RTT: {alice_entry.average_rtt:.2f} ms")
    print("======================================================================")

    # 8. Clean Shutdown
    print("\nStep 7: Shutting down all components...")
    
    # Unregister from Rendezvous
    await rdv_conn.unregister("CIC", "alice", alice_port)
    await rdv_conn.unregister("CIC", "bob", bob_port)
    
    # Close Bob's P2P connection
    await bob_conn.close()
    
    # Cancel tasks
    alice_server_task.cancel()
    bob_server_task.cancel()
    
    await asyncio.gather(alice_server_task, bob_server_task, return_exceptions=True)
    
    # Terminate Rendezvous server process
    rdv_proc.terminate()
    rdv_proc.wait()
    print("✔ All components shutdown cleanly.")
    print("\n🎉 LOCAL DEMO PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user.")
