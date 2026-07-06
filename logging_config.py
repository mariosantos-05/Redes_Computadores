# ================================================================
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
# ARQUIVO: logging_config.py
# ================================================================

import logging
import sys
import readline

class ReadlineConsoleHandler(logging.Handler):
    """
    Handler customizado de Logging que gerencia a impressão de logs em segundo plano
    sem quebrar o prompt visual do usuário gerado pelo 'readline' na CLI.
    
    Quando a CLI está ativa aguardando input (cli_active = True), este handler apaga
    a linha atual, imprime o log de rede e então recria o prompt 'p2p> ' e o que
    o usuário já havia digitado, criando a ilusão de um chat sem interrupções.
    """
    cli_active = False # Flag controlada pelo cli.py para saber se estamos no prompt
    cli_prompt = "p2p> " # Prompt padrão da CLI

    def emit(self, record):
        try:
            msg = self.format(record)
            if ReadlineConsoleHandler.cli_active:
                sys.stdout.write('\r\x1b[K')
                sys.stdout.write(msg + '\n')
                sys.stdout.write(ReadlineConsoleHandler.cli_prompt + readline.get_line_buffer())
                sys.stdout.flush()
                readline.redisplay()
            else:
                sys.stdout.write(msg + '\n')
            sys.stdout.flush()
        except Exception:
            self.handleError(record)

def setup_logger(level_name: str = "INFO") -> logging.Logger:
    """
    Configura o sistema global de logs da aplicação.
    Usa um formato padronizado com timestamp local, nível do log e mensagem.
    """
    numeric_level = getattr(logging, level_name.upper(), logging.INFO)
    
    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    # Remove qualquer handler padrão que o Python tenha colocado
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatação das mensagens: Data Hora | NÍVEL | Mensagem
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Acopla nosso Handler customizado para lidar com a CLI
    console_handler = ReadlineConsoleHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    
    # Tratador de Arquivo (p2p.log)
    file_handler = logging.FileHandler("p2p.log", mode='a')
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
