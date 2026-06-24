import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rendezvous_connection import RendezvousConnection
from peer_server import PeerServer
from peer_connection import PeerConnection
from peer_table import PeerTable
from config import Config

class MockRendezvousServer:
    """In-memory mock of the Rendezvous Server for local self-contained testing."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.peers = {}  # key: (namespace, name, port), val: peer info dict
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await reader.readline()
            if not data:
                return
            msg = json.loads(data.decode().strip())
            msg_type = msg.get("type")
            response = {"status": "ERROR", "message": "Unknown command"}
            
            peer_ip, _ = writer.get_extra_info("peername")

            if msg_type == "REGISTER":
                ns = msg.get("namespace")
                name = msg.get("name")
                port = msg.get("port")
                ttl = msg.get("ttl", 60)
                if ns and name and port:
                    self.peers[(ns, name, port)] = {
                        "name": name,
                        "namespace": ns,
                        "ip": peer_ip,
                        "port": port,
                        "ttl": ttl,
                        "expires_at": asyncio.get_event_loop().time() + ttl
                    }
                    response = {"status": "OK", "port": port, "ttl": ttl}
                    
            elif msg_type == "DISCOVER":
                ns = msg.get("namespace")
                now = asyncio.get_event_loop().time()
                # Clean expired peer entries
                self.peers = {k: v for k, v in self.peers.items() if v["expires_at"] > now}
                
                matched = []
                for (p_ns, _, _), p_info in self.peers.items():
                    if p_ns == ns:
                        info = dict(p_info)
                        info["expires_in"] = int(max(0, info["expires_at"] - now))
                        matched.append(info)
                response = {"status": "OK", "peers": matched}
                
            elif msg_type == "UNREGISTER":
                ns = msg.get("namespace")
                name = msg.get("name")
                port = msg.get("port")
                key = (ns, name, port)
                if key in self.peers:
                    del self.peers[key]
                response = {"status": "OK"}
                
            writer.write((json.dumps(response) + "\n").encode())
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

async def setup_peer(name: str, port: int, namespace: str, rdv_conn: RendezvousConnection, ttl: int):
    """Starts a local peer server and registers it on the Rendezvous server."""
    peer_id = f"{name}@{namespace}"
    table = PeerTable()
    server = PeerServer(peer_id=peer_id, port=port, peer_table=table)
    
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.1)
    
    await rdv_conn.request({
        "type": "REGISTER",
        "namespace": namespace,
        "name": name,
        "port": port,
        "ttl": ttl
    })
    print(f"Peer {peer_id} started on port {port} and registered on Rendezvous.")
    return peer_id, table, server_task

async def main():
    config = Config()

    rdv_host = "127.0.0.1"
    rdv_port = config.rdv_port
    namespace = config.namespace
    ttl = config.rdv_ttl
    
    rdv_server = None
    alice_task = None
    bob_task = None
    bob_conn = None
    bob_listen_task = None

    try:
        # 1. Start the local mock Rendezvous Server
        print("Starting Rendezvous Server...")
        rdv_server = MockRendezvousServer(host=rdv_host, port=rdv_port)
        await rdv_server.start()
        print("Rendezvous Server is up and running.")

        # 2. Setup Rendezvous connection manager
        rdv_conn = RendezvousConnection(rdv_host, rdv_port)

        # 3. Setup and register Alice and Bob
        print("\nSetting up Alice and Bob...")
        alice_id, alice_table, alice_task = await setup_peer("alice", 5002, namespace, rdv_conn, ttl)
        bob_id, bob_table, bob_task = await setup_peer("bob", 5003, namespace, rdv_conn, ttl)

        # 4. Bob queries DISCOVER to locate Alice
        print(f"\nBob executing DISCOVER for namespace '{namespace}'...")
        discover_res = await rdv_conn.request({
            "type": "DISCOVER",
            "namespace": namespace
        })
        
        if discover_res.get("status") == "OK":
            peers = discover_res.get("peers", [])
            print(f"DISCOVER Success: Found {len(peers)} peers.")
            bob_table.update_from_discovery(peers)
        else:
            print("DISCOVER Failed:", discover_res)
            return

        # 5. Bob connects to Alice
        alice_entry = bob_table.get_peer(alice_id)
        if not alice_entry:
            print("Error: Alice not found in Bob's discovered peer list.")
            return
            
        print(f"\nBob connecting to Alice ({alice_id}) at {alice_entry.ip}:{alice_entry.port}...")
        bob_conn = PeerConnection(peer_id=bob_id, peer_table=bob_table)
        bob_conn.keepalive_interval = config.keepalive_interval
        
        await bob_conn.connect(alice_entry.ip, alice_entry.port)
        bob_listen_task = asyncio.create_task(bob_conn.listen())
        print(f"Connection established! Initial status in Bob's table: {alice_entry.status}")
        
        # 6. Wait to observe keepalive exchange and RTT metrics
        sleep_duration = config.keepalive_interval + 2
        print(f"\nKeeping connection alive for {sleep_duration} seconds to observe PING/PONG & RTT updates...")
        await asyncio.sleep(sleep_duration)

        # Print RTT results
        print("\n======================================================================")
        print(f"RESULTS FOR {alice_id}:")
        print(f"RTT History: {alice_entry.rtt_history}")
        if alice_entry.average_rtt is not None:
            print(f"Average RTT: {alice_entry.average_rtt:.2f} ms")
        else:
            print("Average RTT: N/A")
        print("======================================================================")

    finally:
        # 7. Clean Shutdown
        print("\nShutting down all components...")
        
        # Unregister peers from Rendezvous
        if 'rdv_conn' in locals() and rdv_conn:
            try:
                await rdv_conn.unregister(namespace, "alice", 5002)
                await rdv_conn.unregister(namespace, "bob", 5003)
            except Exception:
                pass
        
        # Close Bob's P2P connection
        if bob_conn:
            try:
                await bob_conn.close()
            except Exception:
                pass
        if bob_listen_task:
            bob_listen_task.cancel()
        
        # Stop local peer servers
        for task in (alice_task, bob_task):
            if task:
                task.cancel()
        
        if alice_task or bob_task:
            await asyncio.gather(alice_task, bob_task, return_exceptions=True)
            
        # Stop local mock Rendezvous server
        if rdv_server:
            try:
                await rdv_server.stop()
            except Exception:
                pass
            
        print("Clean shutdown complete.")
        print("\nLOCAL DEMO PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user.")
