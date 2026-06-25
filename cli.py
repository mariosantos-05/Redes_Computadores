import asyncio
import sys
import logging
import uuid
import readline # Enables arrow keys and history for input()
from typing import Dict
from peer_connection import PeerConnection
from peer_table import PeerTable

async def async_input(prompt: str) -> str:
    """Read input from stdin asynchronously using run_in_executor and input()."""
    loop = asyncio.get_event_loop()
    # input() automatically uses readline and handles prompt
    return await loop.run_in_executor(None, input, prompt)

async def cli_loop(
    peer_id: str,
    peer_table: PeerTable,
    outbound_connections: Dict[str, PeerConnection],
    shutdown_event: asyncio.Event
):
    """
    Interactive CLI loop.
    """
    logger = logging.getLogger(__name__)
    await asyncio.sleep(1.0) # Wait for initial logs

    while not shutdown_event.is_set():
        try:
            line = await async_input("p2p> ")
            if shutdown_event.is_set():
                break
            
            line = line.strip()
            if not line:
                continue

            parts = line.split(" ", 2)
            cmd = parts[0].lower()

            if cmd == "/peers":
                filter_arg = parts[1] if len(parts) > 1 else None
                peers = peer_table.get_all_peers()
                print("--- Known Peers ---")
                for p in peers:
                    if filter_arg == "*" or filter_arg is None:
                        pass
                    elif filter_arg.startswith("#"):
                        if p.namespace != filter_arg[1:]:
                            continue
                    print(f"[{p.status}] {p.peer_id} at {p.ip}:{p.port} (TTL: {p.ttl})")
                print("-------------------")

            elif cmd == "/msg":
                if len(parts) < 3:
                    print("Usage: /msg <peer_id> <message>")
                    continue
                target_id = parts[1]
                msg_content = parts[2]
                
                # Check if we have an active connection
                conn = outbound_connections.get(target_id)
                
                # If not, try to establish one
                if conn is None:
                    peer_entry = peer_table.get_peer(target_id)
                    if not peer_entry:
                        print(f"Unknown peer: {target_id}. Try /peers to discover.")
                        continue
                    
                    print(f"Connecting to {target_id} at {peer_entry.ip}:{peer_entry.port}...")
                    conn = PeerConnection(peer_id, peer_table)
                    try:
                        await conn.connect(peer_entry.ip, peer_entry.port)
                        asyncio.create_task(conn.listen())
                        outbound_connections[target_id] = conn
                    except Exception as e:
                        print(f"Failed to connect: {e}")
                        continue
                
                # Send the message
                msg_id = str(uuid.uuid4())
                try:
                    await conn.send_message(msg_content, msg_id, require_ack=True)
                except Exception as e:
                    print(f"Failed to send message: {e}")
                    if target_id in outbound_connections:
                        del outbound_connections[target_id]

            elif cmd == "/pub":
                if len(parts) < 3:
                    print("Usage: /pub <* | #namespace> <message>")
                    continue
                scope = parts[1]
                msg_content = parts[2]
                msg_id = str(uuid.uuid4())
                
                target_peers = []
                for p in peer_table.get_all_peers():
                    if scope == "*":
                        target_peers.append(p)
                    elif scope.startswith("#") and p.namespace == scope[1:]:
                        target_peers.append(p)

                count = 0
                for p in target_peers:
                    # Reuse connection if active
                    conn = outbound_connections.get(p.peer_id)
                    if conn is None:
                        # Attempt to connect
                        conn = PeerConnection(peer_id, peer_table)
                        try:
                            await conn.connect(p.ip, p.port)
                            asyncio.create_task(conn.listen())
                            outbound_connections[p.peer_id] = conn
                        except Exception:
                            continue
                    
                    # Publish over connection
                    try:
                        await conn.publish(msg_content, msg_id, scope)
                        count += 1
                    except Exception:
                        pass
                print(f"Broadcasted to {count} peers.")

            elif cmd == "/conn":
                print("--- Active Outbound Connections ---")
                for pid, conn in outbound_connections.items():
                    print(f"{pid} -> connected")
                print("-----------------------------------")
                
            elif cmd == "/rtt":
                print("--- RTT per Peer ---")
                for p in peer_table.get_all_peers():
                    if p.average_rtt is not None:
                        print(f"{p.peer_id}: {p.average_rtt:.2f} ms")
                    else:
                        print(f"{p.peer_id}: N/A")
                print("--------------------")

            elif cmd == "/reconnect":
                print("Triggering manual reconnect for all disconnected/stale peers...")
                for p in peer_table.get_all_peers():
                    if p.status in ["DISCONNECTED", "STALE"]:
                        p.status = "RECONNECTING"
                        p.next_attempt_allowed_at = 0.0 # Force immediate
                        p.reconnect_attempts = 0

            elif cmd == "/log":
                if len(parts) < 2:
                    print("Usage: /log <DEBUG|INFO|WARNING|ERROR>")
                    continue
                level_str = parts[1].upper()
                num_level = getattr(logging, level_str, None)
                if num_level is not None:
                    logging.getLogger().setLevel(num_level)
                    for handler in logging.getLogger().handlers:
                        handler.setLevel(num_level)
                    print(f"Log level set to {level_str}")
                else:
                    print(f"Invalid log level: {level_str}")

            elif cmd == "/help":
                print("--- Available Commands ---")
                print("  /help                                    : Show this help message")
                print("  /peers [* | #namespace]                  : List known peers")
                print("  /msg <peer_id> <message>                 : Send a direct message to a peer")
                print("  /pub <* | #namespace> <message>          : Broadcast a message to peers")
                print("  /conn                                    : Show active outbound connections")
                print("  /rtt                                     : Show average RTT per peer")
                print("  /reconnect                               : Force immediate reconnect attempts")
                print("  /log <DEBUG|INFO|WARNING|ERROR>          : Change logging level")
                print("  /quit                                    : Exit the application")
                print("--------------------------")

            elif cmd == "/quit":
                print("Initiating clean shutdown...")
                shutdown_event.set()
                break

            else:
                print(f"Unknown command: {cmd}")

        except Exception as e:
            logger.error(f"CLI error: {e}")
            break
