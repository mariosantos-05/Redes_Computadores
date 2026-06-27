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
# ARQUIVO: main.py
# ==============================================================================

import asyncio 
import logging
from peer_server import PeerServer
from peer_table import PeerTable
from rendezvous_connection import RendezvousConnection
from config import Config
from cli import cli_loop
from logging_config import setup_logger
from reconnection_manager import reconnection_loop

async def main():
    """
    Função principal que configura e inicia a execução da aplicação P2P,
    gerenciando os loops de background para registro e descoberta,
    e garantindo um encerramento limpo (UNREGISTER).
    """
    # Instancia o objeto de configuração
    cfg = Config()

    # Configura o logger
    logger = setup_logger(cfg.log_level)
    logger.info("Iniciando aplicação Chat P2P...")

    # 1. Instanciamento da tabela de peers e state da aplicação
    peer_table = PeerTable(max_reconnect_attempts=cfg.max_reconnect_attempts)
    outbound_connections = {}
    shutdown_event = asyncio.Event()

    # 2. Criação do servidor do Peer local:
    peer_id = f"{cfg.name}@{cfg.namespace}"
    port = cfg.listen_port
    server = PeerServer(peer_id, port, peer_table)
    
    # 3. Inicialização da conexão com o Rendezvous:
    rdv_conn = RendezvousConnection(
        cfg.rdv_host,
        cfg.rdv_port
    )

    # 4. Registro inicial no Rendezvous (síncrono/preparatório)
    register_msg = {
        "type": "REGISTER",
        "namespace": cfg.namespace,
        "name": cfg.name,
        "port": port,
        "ttl": cfg.rdv_ttl
    }
    
    logger.info("Registrando peer no servidor Rendezvous...")
    try:
        response = await rdv_conn.request(register_msg)
        if response.get("status") == "OK":
            granted_ttl = response.get("ttl", cfg.rdv_ttl)
            logger.info(f"Registro inicial realizado com sucesso! TTL concedido: {granted_ttl}s")
        else:
            logger.warning(f"Aviso: Servidor Rendezvous retornou erro no registro: {response.get('message')}")
    except Exception as e:
        logger.warning(f"Aviso: Não foi possível conectar ao Rendezvous para registro inicial: {e}")

    # 5. Agendamento dos loops de background (descoberta, re-registro, reconexão e CLI)
    reg_task = asyncio.create_task(
        rdv_conn.registration_loop(
            namespace=cfg.namespace,
            name=cfg.name,
            port=port,
            initial_ttl=cfg.rdv_ttl
        )
    )
    
    disc_task = asyncio.create_task(
        rdv_conn.discovery_loop(
            namespace=cfg.namespace,
            peer_table=peer_table,
            interval=cfg.discover_interval,
            exclude_peer_id=peer_id
        )
    )

    reconn_task = asyncio.create_task(
        reconnection_loop(
            peer_id=peer_id,
            peer_table=peer_table,
            outbound_connections=outbound_connections
        )
    )

    cli_task = asyncio.create_task(
        cli_loop(
            peer_id=peer_id,
            peer_table=peer_table,
            outbound_connections=outbound_connections,
            shutdown_event=shutdown_event
        )
    )

    # 6. Execução do servidor local TCP com tratamento de saída limpa
    # Inicia o servidor local explicitamente sem bloquear a thread principal
    server_runner = await asyncio.start_server(
        server.handle_client,
        "0.0.0.0",
        server.port,
        limit=32768
    )
    logger.info(f"Ouvindo conexões (inbound) na porta {server.port}")

    try:
        # Aguarda até que o evento de desligamento seja acionado (ex: pelo comando /quit na CLI)
        await shutdown_event.wait()
    except asyncio.CancelledError:
        logger.info("Sinal de encerramento recebido (CancelledError).")
    finally:
        logger.info("Iniciando encerramento limpo...")
        
        # Envia BYE para todas conexões de saída ativas
        for pid, conn in outbound_connections.items():
            logger.info(f"Fechando conexão de saída (outbound) com {pid}...")
            await conn.disconnect(reason="Cliente está sendo desligado (shutdown)")
            await conn.close()

        # Cancela loops assíncronos que estão rodando em background
        reg_task.cancel()
        disc_task.cancel()
        reconn_task.cancel()
        cli_task.cancel()
        
        await asyncio.gather(reg_task, disc_task, reconn_task, cli_task, return_exceptions=True)

        # Para o servidor TCP
        server_runner.close()
        for task in list(server.active_tasks):
            task.cancel()
        for w in list(server.active_connections):
            try:
                w.close()
            except Exception:
                pass
        await server_runner.wait_closed()

        # Envio de UNREGISTER
        try:
            await rdv_conn.unregister(
                namespace=cfg.namespace,
                name=cfg.name,
                port=port
            )
            logger.info("Remoção de registro (UNREGISTER) concluída com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao tentar remover registro (UNREGISTER): {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    print("\n[Main] Aplicação encerrada.")