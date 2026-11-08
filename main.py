import os
import io
import csv
import sys
import signal
import boto3
import psutil
import speedtest
import mysql.connector
from getpass import getpass
from datetime import datetime
from dotenv import load_dotenv
from getmac import get_mac_address
from botocore.exceptions import BotoCoreError, ClientError
from mysql.connector import Error, errorcode
from apscheduler.schedulers.blocking import BlockingScheduler

# === Variáveis de Ambiente e Constantes globais === #
load_dotenv()
 
INTERVALO_COLETA = 5       # 30s entre coletas
INTERVALO_SPEEDTEST = 300  # 300s(5min) entre speedtests
CAPTURAS_POR_ARQUIVO = 5   # 20 x INTERVALO_COLETA = 10min de captura p/ CSV
 
LOG_SEVERIDADE = {"info": "INFO", "aviso": "AVISO", "erro": "ERRO"}
MAX_NUCLEOS = 16
 
_mac = get_mac_address()
if not _mac:
    raise SystemExit("Não foi possível obter o endereço MAC desta máquina.")
ENDERECO_MAC = _mac.upper()
ENDERECO_MAC_LIMPO = ENDERECO_MAC.replace(":", "")
 
LOG_DIR = "./flowtech-logs"
BUCKET_NOME = os.getenv("BUCKET_NAME")
BUCKET_PREFIXO = "01-bronze"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connection_timeout": 10,
}

CSV_BUFFER = io.StringIO()
CSV_WRITER = None
QTD_CAPTURAS = 0
ULTIMO_SPEEDTEST = {}

# === Função Principal === #
def main():
    print("=== [ FlowTech Soluções Tecnológicas ] ===")
 
    if not BUCKET_NOME:
        gerar_log("erro", "Variável BUCKET_NAME não definida no .env")
        return
 
    # 1. Identifica o Usuário
    print("Autentique-se:")
 
    email = input("E-mail: ")
    senha = getpass("Senha: ", echo_char="*")
 
    usuario = autenticar(email, senha)
    if not usuario:
        gerar_log("info", "Encerrando programa...")
        return
 
    # 2. Identifica a Máquina
    embarcado = buscar_embarcado(endereco_mac=ENDERECO_MAC, empresa_id=usuario["empresa_id"])
    if not embarcado:
        gerar_log("info", "Encerrando programa...")
        return
 
    if embarcado["status"] == 0:
        gerar_log("info", "O embarcado está desativado no sistema. Altere o status pela dashboard")
        gerar_log("info", "Encerrando programa...")
        return
 
    # 3. Inicia o monitoramento de recursos do embarcado
    iniciar_monitoramento()

# === Funções Auxiliares === #
def mensagem_erro(err):
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:   # 1045
        return "Usuário ou senha do banco inválidos."
    if err.errno == errorcode.ER_BAD_DB_ERROR:          # 1049
        return f"Banco '{DB_CONFIG['database']}' não existe."
    if err.errno in (errorcode.CR_CONN_ERROR,           # 2003
                     errorcode.CR_CONNECTION_ERROR,     # 2002
                     errorcode.CR_SERVER_GONE_ERROR,    # 2006
                     errorcode.CR_SERVER_LOST):         # 2013
        return "Não foi possível conectar ao servidor MySQL (host/porta/rede)."
    return f"Erro inesperado no banco ({err.errno}): {err.msg}"

def gerar_log(severidade: str, msg: str, print_log: bool = True):
    now = datetime.now()
    log = f"[{now:%Y-%m-%d %H:%M:%S}] [{LOG_SEVERIDADE.get(severidade, 'INFO')}] {msg}"
 
    os.makedirs(LOG_DIR, exist_ok=True)
    nome_arquivo = f"{ENDERECO_MAC_LIMPO}_{now:%Y%m%d}.log"
    with open(f"{LOG_DIR}/{nome_arquivo}", "a", encoding="utf-8") as f:
        f.write(log + "\n")
 
    if print_log:
        print(log)

def linha_para_dict(cursor, linha):
    return {
        coluna: valor.decode("utf-8") if isinstance(valor, (bytes, bytearray)) else valor
        for coluna, valor in zip(cursor.column_names, linha)
    }

def autenticar(email: str, senha: str):
    sql = "SELECT empresa_id, nome FROM usuario WHERE email = ? AND senha = SHA2(?, 256)"
 
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(prepared=True)
 
        cursor.execute(sql, (email, senha))
        resultado = cursor.fetchone()
 
        if resultado is None:
            gerar_log("erro", "Nenhum usuário com as credenciais fornecidas pôde ser encontrado")
            return None
 
        usuario = linha_para_dict(cursor, resultado)
        gerar_log("info", f"Autenticação concluída. Seja bem-vindo {usuario['nome']}")
        return usuario
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

def buscar_embarcado(endereco_mac: str, empresa_id: str):
    sql = "SELECT * FROM embarcado WHERE endereco_mac = ? AND empresa_id = ?"
 
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(prepared=True)
 
        cursor.execute(sql, (endereco_mac, empresa_id))
        resultado = cursor.fetchone()
 
        if resultado is None:
            gerar_log("erro", "O embarcado não foi cadastrado pela sua empresa")
            return None
 
        gerar_log("info", "O embarcado está cadastrado. Prosseguindo...")
        return linha_para_dict(cursor, resultado)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

def iniciar_monitoramento():
    gerar_log("info", "Iniciando coleta de métricas do embarcado...")
 
    scheduler = BlockingScheduler()
    now = datetime.now()
 
    # max_instances=1: nunca roda duas execuções da mesma tarefa ao mesmo tempo.
    # coalesce=True: se uma execução atrasar, as perdidas viram uma só.
    scheduler.add_job(
        coletar_metricas, "interval", seconds=INTERVALO_COLETA, next_run_time=now,
        max_instances=1, coalesce=True, misfire_grace_time=INTERVALO_COLETA,
    )

    # O speedtest roda em outra tarefa, pra não atrasar a coleta.
    scheduler.add_job(
        atualizar_speedtest, "interval", seconds=INTERVALO_SPEEDTEST, next_run_time=now,
        max_instances=1, coalesce=True, misfire_grace_time=INTERVALO_SPEEDTEST,
    )
 
    # systemd/docker encerram com SIGTERM: trata igual ao Ctrl+C para dar tempo de enviar o buffer.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
 
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        gerar_log("info", "Encerrando monitoramento...")
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=True)  # espera as tarefas em andamento terminarem
        if QTD_CAPTURAS > 0:
            gerar_log("info", "Enviando capturas restantes ao S3...")
            enviar_dados_s3()

def medir_velocidade():
    try:
        st = speedtest.Speedtest()
        st.get_best_server()
        download = st.download()
        upload = st.upload()
        return download, upload, st.results.ping
    except Exception as e:
        gerar_log("aviso", f"Falha no speedtest: {e}")
        return None, None, None

def atualizar_speedtest():
    resultado = medir_velocidade()
    if resultado[0] is not None:
        ULTIMO_SPEEDTEST["resultado"] = resultado

def coletar_metricas():
    global QTD_CAPTURAS
 
    try:
        for _ in psutil.process_iter(["cpu_percent"]):
            pass
 
        # CPU – métricas:
        qtd_cpu_fisica = psutil.cpu_count(logical=False)
        qtd_cpu_logica = psutil.cpu_count(logical=True)
 
        cpu_uso_por_nucleo = psutil.cpu_percent(percpu=True, interval=1)
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
 
        # Speedtest: consome o último resultado pronto (se houver). Se nenhum speedtest
        # terminou desde a última captura, os campos ficam None.
        download, upload, latencia = ULTIMO_SPEEDTEST.pop("resultado", (None, None, None))
 
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        for i in range(MAX_NUCLEOS):
            captura[f"cpu_{i + 1}"] = cpu_uso_por_nucleo[i] if i < len(cpu_uso_por_nucleo) else None
 
        adicionar_captura(captura)
        gerar_log("info", f"Coleta realizada com sucesso ({QTD_CAPTURAS}/{CAPTURAS_POR_ARQUIVO} no buffer)")
 
        if QTD_CAPTURAS >= CAPTURAS_POR_ARQUIVO:
            enviar_dados_s3()
    except Exception as e:
        # Uma coleta ruim não pode derrubar o monitoramento inteiro.
        gerar_log("erro", f"Falha na coleta de métricas: {e}")

def adicionar_captura(captura: dict):
    global CSV_WRITER, QTD_CAPTURAS
 
    if CSV_WRITER is None:
        CSV_WRITER = csv.DictWriter(CSV_BUFFER, fieldnames=list(captura.keys()), delimiter=";", lineterminator="\n")
        CSV_WRITER.writeheader()
 
    CSV_WRITER.writerow(captura)
    QTD_CAPTURAS += 1

def limpar_buffer():
    global CSV_WRITER, QTD_CAPTURAS
 
    CSV_BUFFER.seek(0)
    CSV_BUFFER.truncate(0)
    CSV_WRITER = None
    QTD_CAPTURAS = 0

def enviar_dados_s3():
    now = datetime.now()
 
    nome_arquivo = f"{ENDERECO_MAC_LIMPO}_{now:%H%M}_captura.csv"
    object_key = (
        f"{BUCKET_PREFIXO.strip('/')}"
        f"/ano={now:%Y}/mes={now:%m}/dia={now:%d}/hora={now:%H}"
        f"/{nome_arquivo}"
    )
 
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=BUCKET_NOME,
            Key=object_key,
            Body=CSV_BUFFER.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
    except (BotoCoreError, ClientError) as e:
        gerar_log("erro", f"Falha ao enviar {nome_arquivo} ao S3 (será tentado novamente): {e}")
        return False
 
    gerar_log("info", f"Arquivo s3://{BUCKET_NOME}/{object_key} enviado com sucesso ({QTD_CAPTURAS} capturas)")
    limpar_buffer()
    return True
 
 
if __name__ == "__main__":
    try:
        main()
    except Error as err:
        gerar_log("erro", mensagem_erro(err))