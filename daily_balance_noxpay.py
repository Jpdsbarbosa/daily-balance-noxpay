import paramiko
import json
from time import sleep
import pandas as pd
from datetime import datetime, timedelta
import pygsheets
import os
from threading import Lock
import pytz

# Configurações via variáveis de ambiente
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', "22"))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
url_financial = os.getenv('url_financial')

# Lista de contas com muitas transações
CONTAS_GRANDES = {
    "44B0F69654774D829A00413476711E1C": {
        "timeout": 180,    # Timeout maior para esta conta específica
        "retries": 5,     # Mais tentativas
        "batch_size": 1   # Batch menor para evitar sobrecarga
    },
    "15277CDE747846BB84C2DFCE85DB504B": {
        "timeout": 120,
        "retries": 4,
        "batch_size": 1
    },
    "AB3FF5EA035C48A5864F9B0C6DCC2CC4": {
        "timeout": 120,
        "retries": 4,
        "batch_size": 1
    },
    "EA67B2F52FC342AB8D91E3293229FE0B": {
        "timeout": 120,
        "retries": 4,
        "batch_size": 1
    }
}

class RateLimiter:
    def __init__(self, max_requests=900, time_window=60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = Lock()

    def wait_if_needed(self):
        with self.lock:
            now = datetime.now()
            self.requests = [req_time for req_time in self.requests 
                           if now - req_time < timedelta(seconds=self.time_window)]
            
            if len(self.requests) >= self.max_requests:
                oldest = min(self.requests)
                sleep_time = (oldest + timedelta(seconds=self.time_window) - now).total_seconds()
                if sleep_time > 0:
                    print(f"Rate limit IUGU atingido. Aguardando {sleep_time:.2f} segundos...")
                    sleep(sleep_time)
                self.requests = []
            
            self.requests.append(now)

# Instância global do rate limiter
rate_limiter = RateLimiter()

def connect_ssh():
    print("Conectando ao servidor SSH...")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=SSH_HOST, 
        port=SSH_PORT, 
        username=SSH_USERNAME, 
        password=SSH_PASSWORD,
        allow_agent=False,
        look_for_keys=False,
        timeout=120
    )
    print("Conexão SSH estabelecida com sucesso.")
    return ssh_client

def execute_curl(ssh_client, url, timeout=30):
    account_id = None
    for acc in CONTAS_GRANDES.keys():
        if acc in url:
            account_id = acc
            break
    
    max_retries = 5 if account_id else 2
    wait_time = 15 if account_id else 5  # Espera maior para contas grandes
    
    for attempt in range(max_retries):
        try:
            if "?" in url:
                url += "&limit=50"
            else:
                url += "?limit=50"
                
            curl_cmd = f'curl -s -m {timeout} "{url}" -H "accept: application/json"'
            print(f"Tentativa {attempt + 1}/{max_retries}: Executando consulta...")
            
            stdin, stdout, stderr = ssh_client.exec_command(curl_cmd, timeout=timeout)
            error = stderr.read().decode('utf-8')
            response = stdout.read().decode('utf-8')
            
            if error:
                print(f"Erro no curl: {error}")
                sleep(wait_time)
                continue
                
            if "error code: 504" in response:
                print("Erro 504 detectado, aguardando...")
                sleep(wait_time * 2)  # Dobra o tempo de espera para erro 504
                continue
            
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                print(f"Erro ao decodificar JSON: {response[:200]}...")
                sleep(wait_time)
                continue
                
        except Exception as e:
            print(f"Erro na tentativa {attempt + 1}: {e}")
            sleep(wait_time)
            
    return None

def get_account_balance_large(ssh_client, token, account_id):
    """Função específica para contas com muitas transações"""
    config = CONTAS_GRANDES[account_id]
    timeout = config["timeout"]
    max_retries = config["retries"]
    
    print(f"\nProcessando conta grande: {account_id}")
    
    # Função auxiliar para tentar a requisição com backoff exponencial
    def try_request(url, attempt=1, max_wait=60):
        try:
            sleep_time = min(5 * (2 ** (attempt - 1)), max_wait)
            if attempt > 1:
                print(f"Aguardando {sleep_time} segundos antes da tentativa {attempt}...")
                sleep(sleep_time)
            
            response = execute_curl(ssh_client, url, timeout=timeout)
            if response and "error code: 504" not in str(response):
                return response
        except Exception as e:
            print(f"Erro na tentativa {attempt}: {e}")
        return None

    # Tenta obter o total de transações
    total_transactions = None
    for attempt in range(max_retries):
        print(f"\nTentativa {attempt + 1}/{max_retries} de obter total de transações")
        response = try_request(f"{url_financial}?api_token={token}", attempt + 1)
        
        if response and "transactions_total" in response:
            total_transactions = response["transactions_total"]
            print(f"Total de transações encontrado: {total_transactions}")
            break
    
    if total_transactions is None:
        print("Não foi possível obter o total de transações após todas as tentativas")
        return None

    # Tenta obter o saldo começando do final
    for attempt in range(max_retries):
        print(f"\nTentativa {attempt + 1}/{max_retries} de obter saldo")
        
        # Tenta diferentes posições caso uma falhe
        positions_to_try = [
            total_transactions - 1,  # Última transação
            total_transactions - 50, # 50 transações antes do final
            total_transactions - 100 # 100 transações antes do final
        ]
        
        for position in positions_to_try:
            if position < 0:
                continue
                
            url = f"{url_financial}?api_token={token}&start={position}&limit=1"
            print(f"Tentando posição {position}")
            
            response = try_request(url, attempt + 1)
            
            if response and response.get("transactions"):
                last_transaction = response["transactions"][0]
                saldo_cents = float(last_transaction["balance_cents"]) / 100
                print(f"Saldo encontrado: R$ {saldo_cents:,.2f}")
                return {
                    "Account": account_id,
                    "transactions_total": total_transactions,
                    "saldo_cents": saldo_cents
                }
    
    print("Não foi possível obter o saldo após todas as tentativas")
    return None

def get_account_balance(ssh_client, token, account_id):
    try:
        timeout = 300 if account_id in CONTAS_GRANDES else 30
        max_retries = 5 if account_id in CONTAS_GRANDES else 3
        
        # Primeiro pega o total de transações
        response = execute_curl(ssh_client, f"{url_financial}?api_token={token}", timeout=timeout)
        
        if not response:
            print(f"Token inválido ou erro de conexão para conta {account_id}")
            return {
                "Account": account_id,
                "transactions_total": 0,
                "saldo_cents": 0
            }
            
        if response.get("transactions_total", 0) == 0:
            print(f"Conta {account_id} não possui transações")
            return {
                "Account": account_id,
                "transactions_total": 0,
                "saldo_cents": 0
            }
            
        total_transactions = response["transactions_total"]
        print(f"Total de transações: {total_transactions}")
        
        # Se tem transações, pega as últimas
        start = max(0, total_transactions - 50)
        response = execute_curl(ssh_client, 
                             f"{url_financial}?api_token={token}&start={start}",
                             timeout=timeout)
        
        if response and response.get("transactions"):
            last_transaction = response["transactions"][-1]
            saldo_cents = float(last_transaction["balance_cents"]) / 100
            print(f"Saldo encontrado: R$ {saldo_cents:,.2f}")
            return {
                "Account": account_id,
                "transactions_total": total_transactions,
                "saldo_cents": saldo_cents
            }
        
        print(f"Erro ao obter transações para conta {account_id}")
                
    except Exception as e:
        print(f"Erro ao processar conta {account_id}: {e}")
    
    return {
        "Account": account_id,
        "transactions_total": 0,
        "saldo_cents": 0
    }

def check_trigger(wks_IUGU_subacc):
    """Verifica se a célula B1 contém TRUE para executar o script."""
    try:
        status = wks_IUGU_subacc.get_value("B1")
        return status.strip().upper() == "TRUE"
    except Exception as e:
        print(f"Erro ao verificar trigger: {e}")
        return False

def reset_trigger(wks_IUGU_subacc):
    """Após a execução, redefine a célula B1 para FALSE."""
    try:
        wks_IUGU_subacc.update_value("B1", "FALSE")
    except Exception as e:
        print(f"Erro ao resetar trigger: {e}")

def update_status(wks_IUGU_subacc, status):
    """Atualiza o status de execução na célula A1."""
    try:
        wks_IUGU_subacc.update_value("A1", status)
    except Exception as e:
        print(f"Erro ao atualizar status: {e}")

def check_all_accounts():
    try:
        print("\nIniciando conexão com Google Sheets...")
        # Conexão com Google Sheets
        gc = pygsheets.authorize(service_file="controles.json")
        sh_gateway = gc.open("Gateway")
        wks_subcontas = sh_gateway.worksheet_by_title("Subcontas")
        sh_balance = gc.open("Daily Balance - Nox Pay")
        wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas")

        print("Iniciando atualização...")
        update_status(wks_IUGU_subacc, "Atualizando...")

        # Lê as subcontas do Google Sheets
        df_subcontas = pd.DataFrame(wks_subcontas.get_all_records())

        # Filtra apenas subcontas ativas
        df_subcontas_ativas = df_subcontas[df_subcontas["NOX"] == "SIM"]

        # Conecta ao SSH
        ssh_client = connect_ssh()
        
        # Lista para armazenar resultados
        resultados = []
        
        # Primeiro, processa APENAS as contas grandes
        print("\nProcessando contas grandes primeiro...")
        for account_id in CONTAS_GRANDES.keys():
            conta = df_subcontas_ativas[df_subcontas_ativas["account"] == account_id]
            if conta.empty:
                print(f"Conta grande {account_id} não encontrada ou não está ativa")
                continue
                
            token = conta.iloc[0]["live_token_full"]
            print(f"\nAccount (grande): {account_id}")
            
            # Tenta várias vezes para contas grandes
            for tentativa in range(3):
                resultado = get_account_balance_large(ssh_client, token, account_id)
                if resultado:
                    resultados.append(resultado)
                    print(f"Resultado:")
                    print(f"- Saldo: R$ {resultado['saldo_cents']:,.2f}")
                    print(f"- Total transações: {resultado['transactions_total']}")
                    break
                else:
                    print(f"Tentativa {tentativa + 1} falhou para conta grande {account_id}")
                    sleep(30)  # Espera 30 segundos entre tentativas
            
            sleep(5)  # Pausa entre contas grandes
        
        # Depois processa as contas normais
        print("\nProcessando contas normais...")
        contas_normais = df_subcontas_ativas[~df_subcontas_ativas["account"].isin(CONTAS_GRANDES.keys())]
        batch_size = 3
        total_contas = len(contas_normais)
        
        for i in range(0, total_contas, batch_size):
            batch = contas_normais.iloc[i:i+batch_size]
            print(f"\nProcessando lote {i//batch_size + 1}/{-(-total_contas//batch_size)}")
            
            for _, row in batch.iterrows():
                token = row["live_token_full"]
                account = row["account"]
                
                print(f"Account: {account}")
                resultado = get_account_balance(ssh_client, token, account)
                
                if resultado:
                    resultados.append(resultado)
                    print(f"Resultado:")
                    print(f"- Saldo: R$ {resultado['saldo_cents']:,.2f}")
                    print(f"- Total transações: {resultado['transactions_total']}")
                else:
                    print(f"Conta {account} não retornou dados válidos")
            
            if i + batch_size < total_contas:
                sleep(1)
        
        # Cria DataFrame com resultados
        if resultados:
            df_resultados = pd.DataFrame(resultados)
            
            # Garante a ordem das colunas
            df_resultados = df_resultados[["Account", "transactions_total", "saldo_cents"]]
            
            # Atualiza o Google Sheets
            tz_br = pytz.timezone('America/Sao_Paulo')
            rodado = datetime.now(pytz.UTC).astimezone(tz_br).strftime("%Y-%m-%d %H:%M:%S")
            wks_IUGU_subacc.update_value("A1", f"Última atualização: {rodado}")

            # Exporta para o Google Sheets
            wks_IUGU_subacc.set_dataframe(
                df_resultados, 
                (2,1), 
                encoding='utf-8', 
                copy_head=True
            )
            
            print("\nProcessamento concluído!")
            print(f"Total de contas processadas: {len(resultados)}")
            print(f"Execução concluída: {rodado}")
        else:
            print("\nNenhum resultado válido foi obtido!")
            wks_IUGU_subacc.update_value("A1", "Erro: Nenhum resultado válido obtido")
        
        # Reset do trigger ao final da execução
        reset_trigger(wks_IUGU_subacc)
        
        ssh_client.close()

    except Exception as e:
        print(f"Erro durante a execução: {e}")
        import traceback
        print(traceback.format_exc())
        if 'ssh_client' in locals():
            ssh_client.close()

def main():
    print("\nIniciando loop principal do Daily Balance...")
    while True:
        try:
            current_time = datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))
            print(f"\n{'='*50}")
            print(f"Verificação de trigger em: {current_time}")
            print(f"{'='*50}")

            # Conexão com Google Sheets
            gc = pygsheets.authorize(service_file="controles.json")
            sh_balance = gc.open("Daily Balance - Nox Pay")
            wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas")

            # Verifica o trigger
            print("Verificando status do trigger...")
            if check_trigger(wks_IUGU_subacc):
                print("Trigger ativo! Iniciando atualização...")
                check_all_accounts()
            else:
                print("Trigger não está ativo (B1 = FALSE). Aguardando próxima verificação...")

        except Exception as e:
            print(f"\nERRO CRÍTICO: {e}")
            print("Tentando reiniciar o loop em 60 segundos...")
            import traceback
            print(traceback.format_exc())
            sleep(60)
            continue

        print(f"\nVerificação concluída em: {datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))}")
        print("Aguardando 60 segundos para próxima verificação do trigger...")
        sleep(60)  # Verifica o trigger a cada 1 minuto

if __name__ == "__main__":
    print("Iniciando Daily Balance NOX Pay...")
    main()
