import asyncio
from peer_connection import PeerConnection
from config import Config

async def main():
    """
    Script de teste projetado para validar a conexão direta P2P com o peer de espelhamento (mirror)
    do professor hospedado na nuvem.
    
    O que este teste faz?
    1. Instancia uma conexão com a identidade do local peer.
    2. Conecta ao peer do professor.
       Esse peer do professor foi programado em modo "mirror" (espelho): tudo que ele recebe de você,
       ele responde de volta.
    3. Executa o handshake de conexão e valida a resposta 'HELLO_OK'.
    4. Dispara a escuta em segundo plano para ver o que o espelho nos manda.
    """
    print("Trying to connect...")

    # Instanciamos o objeto de conexão de saída se identificando com nossa identidade configurada
    cfg = Config()
    conn = PeerConnection(f"{cfg.name}@{cfg.namespace}")

    # 1. Estabelece a conexão TCP e efetua o Handshake inicial (envia HELLO e PING)
    # Espera até que o handshake termine e confirme o sucesso recebendo HELLO_OK
    await conn.connect(
        cfg.rdv_host,
        cfg.listen_port
    )

    # 2. Concorrência Assíncrona com asyncio.create_task:
    # A função 'conn.listen()' possui um loop 'while True' infinito para ler dados do socket.
    # Se usássemos 'await conn.listen()', nosso programa ficaria travado ali para sempre e não
    # executaria mais nenhuma linha de código abaixo.
    # O 'asyncio.create_task(conn.listen())' agenda a execução do loop de escuta em segundo plano,
    # permitindo que a corrotina atual ('main') continue rodando imediatamente as próximas instruções.
    asyncio.create_task(
        conn.listen()
    )
    print("Connected!")

    # 3. Manter o programa vivo:
    # Como a tarefa 'listen()' roda em segundo plano, se o nosso script 'main()' terminar agora,
    # o interpretador Python fecha o programa inteiro e encerra a conexão.
    # Usamos 'await asyncio.sleep(5)' para manter o programa em execução por 5 segundos, 
    # tempo suficiente para vermos as respostas sendo recebidas e impressas na tela.
    await asyncio.sleep(5)

# Executa o loop de eventos assíncrono com a função de teste de conexão direta
asyncio.run(main())