import os
import io
import csv
import sys
import json
import signal
import threading
import boto3
import psutil
import speedtest
import mysql.connector
from getpass import getpass
from datetime import datetime
from dotenv import load_dotenv
from getmac import get_mac_address
from mysql.connector import Error, errorcode
from botocore.exceptions import BotoCoreError, ClientError
from apscheduler.schedulers.blocking import BlockingScheduler

# === VARIÁVEIS DE AMBIENTE E CONSTANTES GLOBAIS === #

# Carrega as variáveis de ambiente presentes em .env
load_dotenv()

INTERVALO_COLETA = 30                                   # intervalo entre as coletas, em segundos
CAPTURAS_POR_CICLO = 20                                 # qtd. de capturas por ciclo
DURACAO_CICLO = INTERVALO_COLETA * CAPTURAS_POR_CICLO   # duração de 1 ciclo, em segundos

INTERVALO_SPEEDTEST = DURACAO_CICLO     # intervalo entre realização de testes de velocidade, em segundos
INTERVALO_ENVIO_LOGS = DURACAO_CICLO    # intervalo entre envio dos logs gerados ao S3, em segundos

# Dicionário com os níveis de severidade dos logs
LOG_SEVERIDADE = {
    "info": "INFO", 
    "aviso": "AVISO", 
    "erro": "ERRO"
}

MAX_NUCLEOS = 16    # qtd de núcleos de CPU que o cliente observa durante o monitoramento

# Obtenção do endereço MAC da máquina antes mesmo de rodar a função principal
_mac = get_mac_address()
if not _mac:
    raise SystemExit("Não foi possível obter o endereço MAC desta máquina.")
ENDERECO_MAC = _mac.upper()                         # ex: 1A:2B:3C:4D:5E:6F
ENDERECO_MAC_LIMPO = ENDERECO_MAC.replace(":", "")  # ex: 1A2B3C4D5E6F

LOG_DIR = "./flowtech-logs"     # Diretório onde serão armazenados localmente os logs gerados pelo cliente

BUCKET_NOME = os.getenv("BUCKET_NAME")              # nome do Bucket S3
BUCKET_PREFIXO = "01-bronze"                        # prefixo base do Bucket S3
PREFIXO_METRICAS = f"{BUCKET_PREFIXO}/metricas"     # prefixo base das métricas do Bucket S3
PREFIXO_LOGS = f"{BUCKET_PREFIXO}/logs"             # prefixo base dos logs do Bucket S3

# configurações para conectar com o Banco de Dados MySQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connection_timeout": 10,
}

CSV_BUFFER = io.StringIO()      # buffer em memória para escrever o CSV da captura
CSV_WRITER = None               # escritor do CSV da captura
QTD_CAPTURAS = 0                # qtd de capturas realizadas no ciclo atual
ULTIMO_SPEEDTEST = {}           # dicionário com as informações do último teste de velocidade realizado

LOG_BUFFER = []                 # lista que consolida os logs gerados durante o ciclo 
LOG_LOCK = threading.Lock()     # objeto de lock que bloqueia outras threads de executarem ao ser invocado
CHAVES_ENVIADAS = set()         # conjunto com o nome(chave) dos arquivos CSV e logs enviados ao Bucket S3

# === FUNÇÃO PRINCIPAL (Main thread) === #

# Responsável por identificar ambos o usuário e a máquina em que o cliente está 
# rodando e iniciar o monitoramento.
def main():
    print("=== [ FlowTech Soluções Tecnológicas ] ===")

    # Verifica se BUCKET_NOME foi carregado no contexto global
    if not BUCKET_NOME:
        gerar_log("erro", "Variável BUCKET_NAME não definida no .env")
        return

    # Identifica o usuário durante o primeiro Login
    # se autenticar com sucesso, prossegue a execução do cliente
    # se não, gera um log e encerra o programa.
    print("Autentique-se:")

    email = input("E-mail: ")
    senha = getpass("Senha: ", echo_char="*")

    usuario = autenticar(email, senha)
    if not usuario:
        gerar_log("info", "Encerrando programa...")
        return

    # Identifica a Máquina em que o cliente está rodando
    # se consegue retorná-la utilizando o ENDERECO_MAC, prossegue a execução do cliente
    # se não, gera um log e encerra o programa.
    embarcado = buscar_embarcado(endereco_mac=ENDERECO_MAC, empresa_id=usuario["empresa_id"])
    if not embarcado:
        gerar_log("info", "Encerrando programa...")
        return

    # Se identificou a Máquina, mas a mesma está inativa no Banco de Dados
    # gera logs informativos e encerra o programa
    if embarcado["status"] == 0:
        gerar_log("info", "O embarcado está desativado no sistema. Altere o status pela dashboard")
        gerar_log("info", "Encerrando programa...")
        return

    # Se chegou até aqui, chama a função que agenda o monitoramento dos recursos 
    iniciar_monitoramento()

# === FUNÇÕES AUXILIARES === #

# Responsável por identificar os erros gerados pela lib mysql.connector e
# traduzi-las em mensagens amigáveis.
def mensagem_erro(err):
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        return "Usuário ou senha do banco inválidos."
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        return f"Banco '{DB_CONFIG.get("database")}' não existe."
    if err.errno in (errorcode.CR_CONN_ERROR,
        errorcode.CR_CONNECTION_ERROR,
        errorcode.CR_SERVER_GONE_ERROR,
        errorcode.CR_SERVER_LOST):
        return "Não foi possível conectar ao servidor MySQL (host/porta/rede)."
    return f"Erro inesperado no banco ({err.errno}): {err.msg}"

# Responsável por gerar os logs que o cliente joga, salvá-los internamente,
# adicioná-los ao LOG_BUFFER e, se exigido, printar o log no terminal.
def gerar_log(severidade: str, msg: str, print_log: bool = True):

    # Identifica o horário atual e classifica o log de acordo com a severidade
    # informada.
    now = datetime.now()
    timestamp = f"{now:%Y-%m-%d %H:%M:%S}"
    nivel = LOG_SEVERIDADE.get(severidade, "INFO")

    # Armazena a string padrão para o log armazenado localmente
    log = f"[{timestamp}] [{nivel}] {msg}"

    # Se não existir, cria o diretório de armazenamento de logs local
    # e salva o log gerado internamente usando o seguinte padrão
    # de nomenclatura: <ENDERECO_MAC_LIMPO>_<YYYYMMdd>.log
    # ex: 1A2B3C4D5E6F_20260101.log | para um log gerado em 01/01/2026 
    os.makedirs(LOG_DIR, exist_ok=True)
    nome_arquivo = f"{ENDERECO_MAC_LIMPO}_{now:%Y%m%d}.log"
    with open(f"{LOG_DIR}/{nome_arquivo}", "a", encoding="utf-8") as f:
        f.write(log + "\n")

    # O formato de log enviado ao Bucket S3 é .jsonl (JSON Lines) e não .log
    # então é necessário ajustar o formato para:
    registro = {
        "timestamp": timestamp,
        "endereco_mac": ENDERECO_MAC,
        "severidade": nivel,
        "mensagem": msg,
    }

    # utiliza LOG_LOCK para impedir que funções rodando em threads diferentes, como:
    # coletar_metricas() e atualizar_speedtest() tentem atualizar LOG_BUFFER ao 
    # mesmo tempo que esta função
    with LOG_LOCK:
        LOG_BUFFER.append(json.dumps(registro, ensure_ascii=False))

    # Se exigido, printa o log no terminal
    if print_log:
        print(log)

# Responsável por autenticar o usuário que está tentando se logar ao rodar o cliente
# pela primeira vez, utilizando o email e a senha fornecidos.
def autenticar(email: str, senha: str):
    sql = "SELECT empresa_id, nome FROM usuario WHERE email = %s AND senha = SHA2(%s, 256)"

    conn = None
    cursor = None
    try:
        # Se conecta com o Banco de Dados utilizando as credenciais definidas em
        # DB_CONFIG e diz ao cursor para retornar as linhas como dicionários.

        # OBS: **DB_CONFIG é uma sintaxe utilizada para passar à função .connect()
        # tanto a chave quanto o valor das propriedades definidas no dicionário DB_CONFIG
        # ex: **DB_CONFIG -> {"host": "localhost", ...}
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Executa o SQL e interpola a query previnindo SQL injection
        cursor.execute(sql, (email, senha))

        # Retorna apenas o 1° resultado encontrado
        usuario = cursor.fetchone()

        # Se não encontrar nenhum usuário, gera um log e retorna None
        if usuario is None:
            gerar_log(
                "erro",
                "Nenhum usuário com as credenciais fornecidas pôde ser encontrado",
            )
            return None

        # Se encontrar, gera um log e retorna o usuário encontrado
        gerar_log(
            "info", f"Autenticação concluída. Seja bem-vindo {usuario['nome']}"
        )
        return usuario
    finally:
        # Ao final de tudo, encerra a conexão com o cursor e com o  
        # Banco de Dados se ela ainda estiver aberta
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

# Responsável por identificar no Banco de Dados o embarcado em que o cliente 
# está rodando, utilizando endereço MAC da máquina e o ID da empresa do usuário
# que se autenticou.
def buscar_embarcado(endereco_mac: str, empresa_id: str):
    sql = "SELECT * FROM embarcado WHERE endereco_mac = %s AND empresa_id = %s"

    conn = None
    cursor = None
    try:
        # Se conecta com o Banco de Dados utilizando as credenciais definidas em
        # DB_CONFIG e diz ao cursor para retornar as linhas como dicionários.
        
        # OBS: **DB_CONFIG é uma sintaxe utilizada para passar à função .connect()
        # tanto a chave quanto o valor das propriedades definidas no dicionário DB_CONFIG
        # ex: **DB_CONFIG -> {"host": "localhost", ...}
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Executa o SQL e interpola a query previnindo SQL injection
        cursor.execute(sql, (endereco_mac, empresa_id))

        # Retorna apenas o 1° resultado encontrado
        resultado = cursor.fetchone()

        # Se não encontrar nenhum embarcado, gera um log e retorna None
        if resultado is None:
            gerar_log("erro", "O embarcado não foi cadastrado pela sua empresa")
            return None

        # Se encontrar, gera um log e retorna o embarcado encontrado
        gerar_log("info", "O embarcado está cadastrado. Prosseguindo...")
        return resultado
    finally:
        # Ao final de tudo, encerra a conexão com o cursor e com o  
        # Banco de Dados se ela ainda estiver aberta
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

# Responsável por criar o objeto scheduler que vai agendar a execução das seguintes
# funções:
# - coletar_metricas()
# - atualizar_speedtest()
# - enviar_logs()
# Uma vez agendadas, as funções vão rodar PARALELAMENTE (em diferentes threads)
# e em intervalos bem definidos. 
def iniciar_monitoramento():
    gerar_log("info", "Iniciando coleta de métricas do embarcado...")

    # criação do objeto. Um BlockingScheduler roda no background
    scheduler = BlockingScheduler()
    now = datetime.now()

    # Agendamento de: Captura das métricas com coletar_metricas()
    # intervalo: INTERVALO_COLETA
    # inicia em: now
    # máximos de instâncias da mesma função rodando por vez: 1
    # se atrasar, ignora e passa para o próximo ciclo? Sim
    # se atrasar, roda de novo em: INTERVALO_COLETA
    scheduler.add_job(
        coletar_metricas, "interval", seconds=INTERVALO_COLETA, next_run_time=now,
        max_instances=1, coalesce=True, misfire_grace_time=INTERVALO_COLETA,
    )

    # Agendamento de: Testes de velocidade com atualizar_speedtest()
    # intervalo: INTERVALO_SPEEDTEST
    # inicia em: now
    # máximos de instâncias da mesma função rodando por vez: 1
    # se atrasar, ignora e passa para o próximo ciclo? Sim
    # se atrasar, roda de novo em: INTERVALO_SPEEDTEST
    scheduler.add_job(
        atualizar_speedtest, "interval", seconds=INTERVALO_SPEEDTEST, next_run_time=now,
        max_instances=1, coalesce=True, misfire_grace_time=INTERVALO_SPEEDTEST,
    )

    # Agendamento de: Envio de logs com enviar_logs()
    # intervalo: INTERVALO_ENVIO_LOGS
    # inicia em: now
    # máximos de instâncias da mesma função rodando por vez: 1
    # se atrasar, ignora e passa para o próximo ciclo? Sim
    # se atrasar, roda de novo em: INTERVALO_ENVIO_LOGS
    scheduler.add_job(
        enviar_logs, "interval", seconds=INTERVALO_ENVIO_LOGS,
        max_instances=1, coalesce=True, misfire_grace_time=INTERVALO_ENVIO_LOGS,
    )

    # Se algo pedir para matar o programa (exemplo: SO ou CTRL+C)
    # envia um sinal dizendo: "vou matar, mas só depois de terminar de rodar"
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    try:
        # Inicia os agendamentos definidos acima
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        gerar_log("info", "Encerrando monitoramento...")
    finally:
        # Encerra os agendamentos definidos acima
        if scheduler.running:
            scheduler.shutdown(wait=True)  # espera as tarefas em andamento terminarem

        # Se ainda há alguma captura no buffer, envia ao S3
        if QTD_CAPTURAS > 0:
            gerar_log("info", "Enviando capturas restantes ao S3...")
            enviar_metricas()

# Responsável por realizar os testes de velocidade utilizando a lib speedtest-cli
# testes levam ~40-45 segundos para rodar. 
def medir_velocidade():
    try:
        st = speedtest.Speedtest()
        st.get_best_server()
        download = st.download()
        upload = st.upload()

        # retorna velocidade download, upload e latencia
        return download, upload, st.results.ping
    except Exception as e:
        gerar_log("aviso", f"Falha no speedtest: {e}")
        return None, None, None

# Responsável por chamar a função medir_velocidade() e atualizar ULTIMO_SPEEDTEST
# para o valor mais recente
def atualizar_speedtest():
    resultado = medir_velocidade()
    if resultado[0] is not None:
        ULTIMO_SPEEDTEST["resultado"] = resultado

# Responsável por coletar todas as métricas disponibilizadas pela lib psutil.
def coletar_metricas():
    # Referencia a(s) variável(eis) definida(s) no contexto global
    global QTD_CAPTURAS

    try:
        # chama rapidamente cpu_percent() para cada núcleo da máquina
        # visando impedir o "bug" que retorna 0.0 na primeira execução
        for _ in psutil.process_iter(["cpu_percent"]):
            pass    # não faz nada, só passa adiante

        # CPU – métricas:
        qtd_cpu_fisica = psutil.cpu_count(logical=False)
        qtd_cpu_logica = psutil.cpu_count(logical=True)

        cpu_uso_por_nucleo = psutil.cpu_percent(percpu=True, interval=1)
        
        # cpu_uso calculado a partir de uma média do uso dos núcleos individuais
        cpu_uso = sum(cpu_uso_por_nucleo) / len(cpu_uso_por_nucleo)

        cpu_freq = psutil.cpu_freq()
        cpu_freq_uso = cpu_freq.current if cpu_freq else None
        cpu_freq_total = cpu_freq.max if cpu_freq else None

        load_1m, load_5m, load_15m = psutil.getloadavg()

        # MEMÓRIA – métricas:
        ram = psutil.virtual_memory()
        ram_uso = ram.used
        ram_total = ram.total
        ram_uso_pct = ram.percent

        swp = psutil.swap_memory()
        swap_uso = swp.used
        swap_total = swp.total
        swap_uso_pct = swp.percent

        # DISCO – métricas:
        disco = psutil.disk_usage("/")
        disco_uso = disco.used
        disco_total = disco.total
        disco_uso_pct = disco.percent

        # PROCESSOS – métricas:

        # lista de objetos contendo, para cada processo encontrado:
        # PID, nome, consumo de CPU em % e consumo de Memória RAM em %
        processos = [
            p.info
            for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"])
        ]
        qtd_processos = len(processos)

        maior_processo_cpu = max(processos, key=lambda p: p["cpu_percent"] or 0.0)
        maior_processo_mem = max(processos, key=lambda p: p["memory_percent"] or 0.0)

        # REDE – métricas:
        net_io = psutil.net_io_counters()
        bytes_enviados = net_io.bytes_sent
        bytes_recebidos = net_io.bytes_recv

        pacotes_enviados = net_io.packets_sent
        pacotes_recebidos = net_io.packets_recv

        qtd_pacotes_perdidos_saindo = net_io.dropout
        qtd_pacotes_perdidos_chegando = net_io.dropin

        qtd_erros_enviando = net_io.errout
        qtd_erros_recebendo = net_io.errin

        # Speedtest: consome o último resultado pronto (se houver). 
        # Se nenhum speedtest terminou desde a última captura, os campos ficam None.
        download, upload, latencia = ULTIMO_SPEEDTEST.pop("resultado", (None, None, None))

        # String com o timestamp atual
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Body da uma das linhas do CSV
        captura = {
            "timestamp": timestamp,
            "endereco_mac": ENDERECO_MAC,
            "cpu_uso_pct": cpu_uso,
            "qtd_cpu_fisica": qtd_cpu_fisica,
            "qtd_cpu_logica": qtd_cpu_logica,
            "cpu_freq": cpu_freq_uso,
            "cpu_freq_total": cpu_freq_total,
            "load_avg_1m": load_1m,
            "load_avg_5m": load_5m,
            "load_avg_15m": load_15m,
            "ram_uso": ram_uso,
            "ram_total": ram_total,
            "swap_uso": swap_uso,
            "swap_total": swap_total,
            "disco_uso": disco_uso,
            "disco_total": disco_total,
            "qtd_processos": qtd_processos,
            "maior_processo_cpu": maior_processo_cpu["name"],
            "maior_processo_cpu_pct": maior_processo_cpu["cpu_percent"],
            "maior_processo_ram": maior_processo_mem["name"],
            "maior_processo_ram_pct": maior_processo_mem["memory_percent"],
            "download_mbps": download,
            "upload_mbps": upload,
            "latencia_ms": latencia,
            "package_loss_sent": qtd_pacotes_perdidos_saindo,
            "package_loss_received": qtd_pacotes_perdidos_chegando,
        }

        # Como cada máquina pode ter qtds. diferentes de núcleos, estes campos tem
        # de ser adicionados ao body dinamicamente.
        for i in range(MAX_NUCLEOS):
            captura[f"cpu_{i + 1}"] = cpu_uso_por_nucleo[i] if i < len(cpu_uso_por_nucleo) else None

        # chama adicionar_captura() e gera um log
        adicionar_captura(captura)
        gerar_log("info", f"Coleta realizada com sucesso ({QTD_CAPTURAS}/{CAPTURAS_POR_CICLO} no buffer)")

        # Se a qtd. de capturas do ciclo atual atingiu o máximo permitido por ciclo
        # encerra o ciclo ao chamar enviar_metricas()
        if QTD_CAPTURAS >= CAPTURAS_POR_CICLO:
            enviar_metricas()
    except Exception as e:
        # Se algo falhou durante a coleta, apenas geramos um log com o erro pois
        # uma coleta ruim não pode derrubar o monitoramento todo.
        gerar_log("erro", f"Falha na coleta de métricas: {e}")

# Responsável por consolidar novas linhas e, se estiver vazio, o header do
# arquivo CSV sendo montado em memória (buffer).
def adicionar_captura(captura: dict):
    # Referencia a(s) variável(eis) definida(s) no contexto global
    global CSV_WRITER, QTD_CAPTURAS

    # Se o Buffer estiver vazio, significa que também devemos escrever o header
    if CSV_WRITER is None:
        # As colunas do header são nomeadas de acordo com o nome das chaves
        # do dicionário da captura
        CSV_WRITER = csv.DictWriter(CSV_BUFFER, fieldnames=list(captura.keys()), delimiter=";", lineterminator="\n")
        CSV_WRITER.writeheader()

    # adiciona a nova linha ao CSV em memória e incrementa QTD_CAPTURAS
    CSV_WRITER.writerow(captura)
    QTD_CAPTURAS += 1

# Responsável por resetar CSV_BUFFER, CSV_WRITER e QTD_CAPTURAS para seu
# estado original. 
# Visando o começo de um novo ciclo.
def limpar_buffer():
    global CSV_BUFFER, CSV_WRITER, QTD_CAPTURAS

    CSV_BUFFER = io.StringIO()
    CSV_WRITER = None
    QTD_CAPTURAS = 0

# === FUNÇÕES DE ENVIO AO S3 === #

# Responsável por garantir que a chave, isto é, o nome do arquivo que será enviado
# ao S3 não será repetido.
# Sem essa garantia, se a duração de 1 ciclo cair para menos de 1min, arquivos com
# nomes idênticos serão gerados e os antigos serão sobrescritos no Bucket S3. 
def chave_unica(chave: str):
    # Separa o nome do arquivo de sua extensão
    base, extensao = os.path.splitext(chave)

    candidata = chave
    n = 2

    # Enquanto o nome do arquivo na variável candidata já existir na lista de 
    # chaves enviadas durante a execução do cliente, tentamos um novo nome.
    # ex:
    # CHAVES_ENVIADAS possuí [captura001.csv, captura001_2.csv]
    # candidata vai assumir o valor de: captura001_3.csv
    while candidata in CHAVES_ENVIADAS:
        candidata = f"{base}_{n}{extensao}"
        n += 1

    # Quando, enfim, o nome for válido, retorna-o
    return candidata

# Responsável por enviar arquivos ao Bucket S3, seguindo os parâmetros passados
def enviar_para_s3(prefixo: str, sufixo_nome: str, conteudo: str, content_type: str):
    # Sem Bucket definido, sem envio. retorna.
    if not BUCKET_NOME:
        return None

    # Horário atual
    now = datetime.now()

    # Monta a chave completa a ser enviada ao S3
    # formato padronizado: <ENDERECO_MAC_LIMPO>_<HHMM>.<sufixo>
    # ex:
    # - 1A2B3C4D5E6F_1200_captura.csv   | para Capturas enviadas ao 12:00
    # - 1A2B3C4D5E6F_1201_log.jsonl     | para Logs enviados ao 12:01
    nome_arquivo = f"{ENDERECO_MAC_LIMPO}_{now:%H%M}_{sufixo_nome}"
    object_key = chave_unica(
        f"{prefixo.strip('/')}"
        f"/ano={now:%Y}/mes={now:%m}/dia={now:%d}/hora={now:%H}"
        f"/{nome_arquivo}"
    )

    try:
        # instancia o cliente do S3 e envia o conteúdo passado para a função
        # para o prefixo correto.
        # Espera-se que a máquina tenha as credenciais AWS configuradas no sistema.
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=BUCKET_NOME,
            Key=object_key,
            Body=conteudo.encode("utf-8"),
            ContentType=content_type,
        )
    except (BotoCoreError, ClientError) as e:
        gerar_log("erro", f"Falha ao enviar {nome_arquivo} ao S3: {e}")
        return None

    # Se o envio sucedeu, adiciona a chave enviada à lista global de chaves enviadas
    # e retorna a chave enviada
    CHAVES_ENVIADAS.add(object_key)
    return object_key

# Responsável por enviar ao S3 específicamente as métricas
def enviar_metricas():
    # Chama enviar_para_s3() com os parâmetros adequados
    chave = enviar_para_s3(PREFIXO_METRICAS, "captura.csv", CSV_BUFFER.getvalue(), "text/csv")

    # Se o envio falhar, retorna False
    if chave is None:
        return False

    # Se o envio suceder, gera um log, limpa o CSV em memória (buffer) e retorna True
    gerar_log("info", f"Arquivo s3://{BUCKET_NOME}/{chave} enviado com sucesso ({QTD_CAPTURAS} capturas)")
    limpar_buffer()
    return True

# Responsável por enviar ao S3 específicamente os logs
def enviar_logs():
    # utiliza LOG_LOCK para impedir que funções rodando em threads diferentes, como:
    # coletar_metricas() e atualizar_speedtest() tentem atualizar LOG_BUFFER ao 
    # mesmo tempo que esta função
    with LOG_LOCK:
        # copia tudo de LOG_BUFFER para registros e em seguida limpa LOG_BUFFER
        registros = LOG_BUFFER[:]
        LOG_BUFFER.clear()

    # se depois da cópia, registros está vazio, significa que não há logs a serem
    # enviados para o S3, portanto não faz nada e retorna True
    if not registros:
        return True

    # Chama enviar_para_s3() com os parâmetros adequados
    chave = enviar_para_s3(PREFIXO_LOGS, "log.jsonl", "\n".join(registros) + "\n", "application/x-ndjson")

    # Se falhou, retorna LOG_BUFFER para o estado original e retorna False
    if chave is None:
        with LOG_LOCK:
            LOG_BUFFER[0:0] = registros
        return False

    # Se sucedeu, gera um log e retorna True
    gerar_log("info", f"Arquivo s3://{BUCKET_NOME}/{chave} enviado com sucesso ({len(registros)} registros)")
    return True

# Condicional padrão do Python para impedir que este arquivo seja importado e rodado
# a partir de outros arquivos Python.
# Responsável por chamar a função main() e iniciar todo o fluxo acima.
if __name__ == "__main__":
    try:
        main()
    except Error as err:
        gerar_log("erro", mensagem_erro(err))
    finally:
        enviar_logs()