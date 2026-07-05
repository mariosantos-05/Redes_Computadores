import asyncio
import sys
import os
import io

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import cli
from peer_table import PeerTable, PeerEntry
from peer_connection import PeerConnection

async def main():
    print("--- Running Test 9: P2P CLI Loop & Command Execution ---")
    
    # 1. Setup mock parameters
    peer_id = "local_peer@TEST_NS"
    peer_table = PeerTable()
    
    # Add a mock peer for /peers command check
    mock_peer = PeerEntry(name="alice", namespace="TEST_NS", ip="127.0.0.1", port=5005, ttl=120)
    mock_peer.status = "CONNECTED"
    peer_table.peers[mock_peer.peer_id] = mock_peer
    
    outbound_connections = {}
    inbound_connections = {}
    shutdown_event = asyncio.Event()

    # Add a mock active connection for /conn command check
    class MockPeerConnection:
        def __init__(self, remote_id):
            self.remote_peer_id = remote_id
    outbound_connections["alice@TEST_NS"] = MockPeerConnection("alice@TEST_NS")

    # 2. Mock cli.async_input
    # We want to run: /help, /peers, /conn, /rtt, /quit
    commands = ["/help", "/peers", "/conn", "/rtt", "/quit"]
    
    async def mock_async_input(prompt: str) -> str:
        # Sleep a little to yield to event loop
        await asyncio.sleep(0.01)
        if commands:
            cmd = commands.pop(0)
            return cmd
        return ""

    # Replace the async_input function in cli module
    cli.async_input = mock_async_input

    # 3. Capture stdout to verify printed menus/tables
    old_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()

    try:
        # Run the cli_loop task (which waits 1.0 second on start)
        cli_task = asyncio.create_task(
            cli.cli_loop(
                peer_id=peer_id,
                peer_table=peer_table,
                outbound_connections=outbound_connections,
                inbound_connections=inbound_connections,
                shutdown_event=shutdown_event
            )
        )
        
        # Wait until the CLI loop sets the shutdown event (on /quit)
        await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
        await cli_task
    finally:
        sys.stdout = old_stdout

    # 4. Check the captured output
    output_str = buffer.getvalue()
    print("Captured CLI Output:")
    print(output_str)

    # Assertions
    assert "--- Comandos Disponíveis ---" in output_str, "Help menu should be displayed"
    assert "--- Peers Conhecidos ---" in output_str, "Peers list should be displayed"
    assert "[CONNECTED] alice@TEST_NS" in output_str, "Mock peer should be listed as CONNECTED (in English)"
    assert "--- Conexões Ativas de Saída (Outbound) ---" in output_str, "Outbound connections list should be displayed"
    assert "alice@TEST_NS -> Connected" in output_str, "Alice should be shown as Connected in English in /conn output"
    assert "--- RTT Médio por Peer ---" in output_str, "RTT list should be displayed"
    assert "Iniciando encerramento limpo..." in output_str, "Shutdown prompt should be displayed on /quit"
    assert shutdown_event.is_set(), "Shutdown event should be set by /quit"

    print("Test 9 Passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
