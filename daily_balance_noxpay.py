import paramiko
import json
from time import sleep
import pandas as pd
from datetime import datetime, timedelta
import pygsheets
import os
from threading import Lock

# Configurações via variáveis de ambiente
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', "22"))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
url_financial = "https://api.iugu.com/v1/accounts/financial"

# Lista de contas com muitas transações
CONTAS_GRANDES = {
    "44B0F69654774D829A00413476711E1C": {"timeout": 180, "retries": 5},  # Conta mais importante
    "15277CDE747846BB84C2DFCE85DB504B": {"timeout": 180, "retries": 5},
    "AB3FF5EA035C48A5864F9B0C6DCC2CC4": {"timeout": 180, "retries": 5},
    "EA67B2F52FC342AB8D91E3293229FE0B": {"timeout": 180, "retries": 5}
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

def execute_curl(ssh_client, url, timeout=30, max_retries=2):
    for attempt in range(max_retries):
        try:
            curl_cmd = f'curl -s -m {timeout} "{url}" -H "accept: application/json"'
            print(f"Tentativa {attempt + 1}/{max_retries}: Executando consulta...")
            
            stdin, stdout, stderr = ssh_client.exec_command(curl_cmd, timeout=timeout)
            error = stderr.read().decode('utf-8')
            response = stdout.read().decode('utf-8')
            
            if error:
                print(f"Erro no curl: {error}")
                sleep(3)
                continue
                
            if "error code: 504" in response:
                print("Erro 504 detectado, aguardando...")
                sleep(5)
                continue
                
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                print(f"Erro ao decodificar JSON: {response[:200]}...")
                sleep(3)
                continue
                
        except Exception as e:
            print(f"Erro na tentativa {attempt + 1}: {e}")
            sleep(3)
            
    return None

def get_account_balance_large(ssh_client, token, account_id):
    """Função específica para contas com muitas transações"""
    try:
        config = CONTAS_GRANDES[account_id]
        timeout = config["timeout"]
        max_retries = config["retries"]
        
        # Verifica rate limit
        rate_limiter.wait_if_needed()
        
        # Faz apenas UMA chamada, ordenando por data de criação decrescente
        url = f"{url_financial}?api_token={token}&limit=1&sort=-created_at"
        response = execute_curl(ssh_client, url, timeout=timeout, max_retries=max_retries)
        
        if response and response.get("transactions"):
            last_transaction = response["transactions"][0]  # Pega a primeira (mais recente)
            saldo_cents = float(last_transaction["balance_cents"]) / 100
            total_transactions = response.get("total_items", 0)
            return {
                "Account": account_id,
                "transactions_total": total_transactions,
                "saldo_cents": saldo_cents
            }
    except Exception as e:
        print(f"Erro ao processar conta grande {account_id}: {e}")
    
    return None

def get_account_balance(ssh_client, token, account_id):
    """Função para contas normais"""
    try:
        # Verifica rate limit
        rate_limiter.wait_if_needed()
        
        # Faz apenas UMA chamada, ordenando por data de criação decrescente
        url = f"{url_financial}?api_token={token}&limit=1&sort=-created_at"
        response = execute_curl(ssh_client, url)
        
        if response and response.get("transactions"):
            last_transaction = response["transactions"][0]  # Pega a primeira (mais recente)
            saldo_cents = float(last_transaction["balance_cents"]) / 100
            total_transactions = response.get("total_items", 0)
            return {
                "Account": account_id,
                "transactions_total": total_transactions,
                "saldo_cents": saldo_cents
            }
    except Exception as e:
        print(f"Erro ao processar conta {account_id}: {e}")
    
    return None

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

        # Verifica o trigger
        print("Verificando trigger...")
        if not check_trigger(wks_IUGU_subacc):
            print("Trigger não está ativo (B1 = FALSE). Encerrando execução.")
            return

        print("Trigger ativo! Iniciando atualização...")
        update_status(wks_IUGU_subacc, "Atualizando...")

        # Lê as subcontas do Google Sheets
        df_subcontas = pd.DataFrame(wks_subcontas.get_all_records())
        
        # Filtra apenas subcontas ativas
        df_subcontas_ativas = df_subcontas[df_subcontas["NOX"] == "SIM"]

        # Conecta ao SSH
        ssh_client = connect_ssh()
        
        # Lista para armazenar resultados
        resultados = []
        
        # Separa contas grandes das normais
        contas_grandes = df_subcontas_ativas[df_subcontas_ativas["account"].isin(CONTAS_GRANDES.keys())]
        contas_normais = df_subcontas_ativas[~df_subcontas_ativas["account"].isin(CONTAS_GRANDES.keys())]
        
        # Processa primeiro as contas grandes
        print("\nProcessando contas com muitas transações...")
        for _, row in contas_grandes.iterrows():
            token = row["live_token_full"]
            account = row["account"]
            
            print(f"\nAccount (grande): {account}")
            resultado = get_account_balance_large(ssh_client, token, account)
            
            if resultado:
                resultados.append(resultado)
                print(f"Resultado:")
                print(f"- Saldo: R$ {resultado['saldo_cents']:,.2f}")
                print(f"- Total transações: {resultado['transactions_total']}")
            else:
                print(f"Conta {account} não retornou dados válidos")
            
            sleep(3)  # Pausa entre contas grandes
        
        # Processa contas normais em lotes
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
            rodado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        
        # Reset do trigger
        reset_trigger(wks_IUGU_subacc)
        
        ssh_client.close()
        
    except Exception as e:
        print(f"Erro durante a execução: {e}")
        import traceback
        print(traceback.format_exc())
        if 'ssh_client' in locals():
            ssh_client.close()

if __name__ == "__main__":
    print("Iniciando verificação...")
    check_all_accounts()