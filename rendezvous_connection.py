# ==============================================================================
# PROJETO: Chat P2P (Versão Revisada)
# DISCIPLINA: Redes de Computadores
# INSTITUIÇÃO: Universidade de Brasília (UnB) - CIC
# GRUPO: 2
#
# MEMBROS DA EQUIPE:
# - Gabriel Gonçalves Caldo (Matrícula: 231034627)
# - Daniel Rodrigues de Abreu (Matrícula: 241038540)
# - Mario Augusto Vieira dos Santos (Matrícula: 231035778)
#
# ARQUIVO: rendezvous_connection.py
# ==============================================================================

import asyncio
import logging
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
        logger = logging.getLogger(__name__)
        unregister_msg = {
            "type": "UNREGISTER",
            "namespace": namespace,
            "name": name,
            "port": port
        }
        logger.info(f"[Rendezvous] Removendo registro (UNREGISTER) do peer {name}@{namespace} na porta {port}...")
        try:
            return await self.request(unregister_msg)
        except Exception as e:
            logger.error(f"[Rendezvous] Falha ao enviar requisição UNREGISTER: {e}")
            return {"status": "ERROR", "message": str(e)}

    async def registration_loop(self, namespace: str, name: str, port: int, initial_ttl: int):
        """
        Loop de background para re-registrar periodicamente o peer antes que o TTL expire.
        """
        logger = logging.getLogger(__name__)
        ttl = initial_ttl
        while True:
            try:
                # Intervalo seguro: 80% do TTL atual, garantindo re-registro bem antes de expirar
                sleep_time = max(5.0, ttl * 0.8)
                logger.debug(f"[Rendezvous] Próxima atualização de registro em {sleep_time:.1f} segundos (TTL={ttl}).")
                await asyncio.sleep(sleep_time)

                register_msg = {
                    "type": "REGISTER",
                    "namespace": namespace,
                    "name": name,
                    "port": port,
                    "ttl": initial_ttl
                }
                logger.info(f"[Rendezvous] Renovando registro (REGISTER) do peer {name}@{namespace} na porta {port}...")
                response = await self.request(register_msg)

                if response.get("status") == "OK":
                    ttl = response.get("ttl", initial_ttl)
                    logger.debug(f"[Rendezvous] Renovação de registro bem sucedida. TTL concedido={ttl}.")
                else:
                    error_msg = response.get("message", "erro desconhecido")
                    logger.warning(f"[Rendezvous] Falha na renovação de registro: {error_msg}. Tentando novamente em 10 segundos...")
                    # Reduz temporariamente o TTL virtual para tentar novamente rápido
                    ttl = 12.5  # 12.5 * 0.8 = 10s sleep
            except asyncio.CancelledError:
                logger.info("[Rendezvous] Loop de renovação de registro cancelado.")
                break
            except Exception as e:
                logger.error(f"[Rendezvous] Erro no loop de renovação de registro: {e}. Tentando novamente em 10 segundos...")
                ttl = 12.5

    async def discovery_loop(self, namespace: str, peer_table, interval: float, exclude_peer_id: str = None):
        """
        Loop de background para buscar periodicamente novos peers no namespace e atualizar a tabela.
        """
        logger = logging.getLogger(__name__)
        while True:
            try:
                discover_msg = {
                    "type": "DISCOVER"
                }
                logger.debug("[Rendezvous] Solicitando descoberta (DISCOVER) global...")
                response = await self.request(discover_msg)

                if response.get("status") == "OK":
                    peers = response.get("peers", [])
                    if exclude_peer_id:
                        peers = [p for p in peers if f"{p.get('name')}@{p.get('namespace')}" != exclude_peer_id]
                    logger.debug(f"[Rendezvous] Descoberta bem sucedida. Encontrado(s) {len(peers)} peer(s) registrado(s).")
                    new_peers = peer_table.update_from_discovery(peers)
                    if new_peers:
                        for np in new_peers:
                            logger.info(f"[Rendezvous] Novo peer descoberto: {np.peer_id} em {np.ip}:{np.port}")
                else:
                    error_msg = response.get("message", "erro desconhecido")
                    logger.warning(f"[Rendezvous] Falha na descoberta: {error_msg}")
            except asyncio.CancelledError:
                logger.info("[Rendezvous] Loop de descoberta (DISCOVER) cancelado.")
                break
            except Exception as e:
                logger.error(f"[Rendezvous] Erro no loop de descoberta: {e}")

            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("[Rendezvous] Loop de descoberta cancelado durante a pausa.")
                break