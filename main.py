import asyncio 
from peer_server import PeerServer
from peer_table import PeerTable
from rendezvous_connection import RendezvousConnection
from config import Config

async def main():
    """
    Função principal que configura e inicia a execução da aplicação P2P,
    gerenciando os loops de background para registro e descoberta,
    e garantindo um encerramento limpo (UNREGISTER).
    """
    # Instancia o objeto de configuração
    cfg = Config()

    # 1. Instanciamento da tabela de peers
    peer_table = PeerTable()

    # 2. Criação do servidor do Peer local:
    peer_id = f"{cfg.name}@{cfg.namespace}"
    port = cfg.tcp_port
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
    
    print("[Main] Registrando peer no servidor Rendezvous...")
    try:
        response = await rdv_conn.request(register_msg)
        if response.get("status") == "OK":
            granted_ttl = response.get("ttl", cfg.rdv_ttl)
            print(f"[Main] Registro inicial realizado com sucesso! TTL concedido: {granted_ttl}s")
        else:
            print(f"[Main] Aviso: Servidor Rendezvous retornou erro no registro: {response.get('message')}")
    except Exception as e:
        print(f"[Main] Aviso: Não foi possível conectar ao Rendezvous para registro inicial: {e}")

    # 5. Agendamento dos loops de background (descoberta e re-registro)
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
            interval=cfg.discover_interval
        )
    )

    # 6. Execução do servidor local TCP com tratamento de saída limpa
    try:
        # Inicializa o socket local do servidor e serve conexões de entrada
        await server.start()
    except (asyncio.CancelledError, KeyboardInterrupt):
        print("\n[Main] Sinal de encerramento recebido.")
    finally:
        # Cancelamento das tarefas de background de forma limpa
        print("[Main] Cancelando tarefas de background...")
        reg_task.cancel()
        disc_task.cancel()
        
        # Aguarda o cancelamento das tarefas se encerrarem
        await asyncio.gather(reg_task, disc_task, return_exceptions=True)

        # Envio de UNREGISTER ao servidor Rendezvous para remover nossa presença da lista ativa
        try:
            await rdv_conn.unregister(
                namespace=cfg.namespace,
                name=cfg.name,
                port=port
            )
            print("[Main] Remoção de registro (UNREGISTER) concluída com sucesso.")
        except Exception as e:
            print(f"[Main] Erro ao tentar remover registro (UNREGISTER): {e}")

# Inicialização do Loop de Eventos com captura amigável de KeyboardInterrupt externo
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Aplicação encerrada de forma amigável pelo usuário.")