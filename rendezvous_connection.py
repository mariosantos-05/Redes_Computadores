import asyncio
from message_router import decode_message
from message_router import encode_message

class RendezvousConnection:
    """
    Classe que representa e gerencia a comunicação com o Servidor Rendezvous.
    Seguindo o protocolo especificado, cada interação com o Rendezvous é feita
    através de uma conexão TCP curta de comando único (envia uma linha e encerra).
    """

    def __init__(self, host, port):
        # Endereço IP/Host do servidor Rendezvous (ex: '45.171.101.167')
        self.host = host
        # Porta TCP do servidor Rendezvous (ex: 8080)
        self.port = port

    async def request(self, msg):
        """
        Envia uma requisição única para o servidor Rendezvous, aguarda o retorno,
        fecha a conexão imediatamente e retorna a resposta decodificada.
        """
        # Abre uma nova conexão TCP com o servidor Rendezvous
        reader, writer = await asyncio.open_connection(
            self.host,
            self.port
        )

        # Envia a mensagem codificada (JSON + '\n')
        writer.write(
            encode_message(msg)
        )

        # Garante o envio físico dos dados
        await writer.drain()

        # Lê a única linha de resposta enviada pelo servidor
        data = await reader.readline()

        # Decodifica a resposta JSON para dicionário Python
        response = decode_message(data)

        # Fecha a conexão TCP imediatamente após obter a resposta (requisito do protocolo)
        writer.close()
        await writer.wait_closed()

        return response

    async def unregister(self, namespace: str, name: str, port: int) -> dict:
        """
        Envia uma requisição UNREGISTER para o servidor Rendezvous para remover o cadastro do peer.
        """
        unregister_msg = {
            "type": "UNREGISTER",
            "namespace": namespace,
            "name": name,
            "port": port
        }
        print(f"[Rendezvous] Unregistering peer {name}@{namespace} on port {port}...")
        try:
            return await self.request(unregister_msg)
        except Exception as e:
            print(f"[Rendezvous] Failed to send UNREGISTER: {e}")
            return {"status": "ERROR", "message": str(e)}

    async def registration_loop(self, namespace: str, name: str, port: int, initial_ttl: int):
        """
        Loop de background para re-registrar periodicamente o peer antes que o TTL expire.
        """
        ttl = initial_ttl
        while True:
            try:
                # Intervalo seguro: 80% do TTL atual, garantindo re-registro bem antes de expirar
                sleep_time = max(5.0, ttl * 0.8)
                print(f"[Rendezvous] Next registration update in {sleep_time:.1f} seconds (TTL={ttl}).")
                await asyncio.sleep(sleep_time)

                register_msg = {
                    "type": "REGISTER",
                    "namespace": namespace,
                    "name": name,
                    "port": port,
                    "ttl": initial_ttl
                }
                print(f"[Rendezvous] Re-registering peer {name}@{namespace} on port {port}...")
                response = await self.request(register_msg)

                if response.get("status") == "OK":
                    ttl = response.get("ttl", initial_ttl)
                    print(f"[Rendezvous] Re-registration successful. Granted TTL={ttl}.")
                else:
                    error_msg = response.get("message", "unknown error")
                    print(f"[Rendezvous] Re-registration failed: {error_msg}. Retrying in 10 seconds...")
                    # Reduz temporariamente o TTL virtual para tentar novamente rápido
                    ttl = 12.5  # 12.5 * 0.8 = 10s sleep
            except asyncio.CancelledError:
                print("[Rendezvous] Registration loop cancelled.")
                break
            except Exception as e:
                print(f"[Rendezvous] Error in registration loop: {e}. Retrying in 10 seconds...")
                ttl = 12.5

    async def discovery_loop(self, namespace: str, peer_table, interval: float):
        """
        Loop de background para buscar periodicamente novos peers no namespace e atualizar a tabela.
        """
        while True:
            try:
                discover_msg = {
                    "type": "DISCOVER",
                    "namespace": namespace
                }
                print(f"[Rendezvous] Querying DISCOVER for namespace '{namespace}'...")
                response = await self.request(discover_msg)

                if response.get("status") == "OK":
                    peers = response.get("peers", [])
                    print(f"[Rendezvous] Discovery successful. Found {len(peers)} registered peer(s).")
                    new_peers = peer_table.update_from_discovery(peers)
                    if new_peers:
                        for np in new_peers:
                            print(f"[Rendezvous] New peer discovered: {np.peer_id} at {np.ip}:{np.port}")
                else:
                    error_msg = response.get("message", "unknown error")
                    print(f"[Rendezvous] Discovery failed: {error_msg}")
            except asyncio.CancelledError:
                print("[Rendezvous] Discovery loop cancelled.")
                break
            except Exception as e:
                print(f"[Rendezvous] Error in discovery loop: {e}")

            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                print("[Rendezvous] Discovery loop cancelled during sleep.")
                break