import paramiko
import json
from time import sleep
import pandas as pd
from datetime import datetime, timedelta
import pygsheets
import os
from threading import Lock
import time

# Configurações via variáveis de ambiente
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', "22"))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
url_financial = "https://api.iugu.com/v1/accounts/financial"

class RateLimiter:
    def __init__(self, max_requests=900, time_window=60):  # 900 para ter margem de segurança
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = Lock()

    def wait_if_needed(self):
        with self.lock:
            now = datetime.now()
            # Remove requisições antigas
            self.requests = [req_time for req_time in self.requests 
                           if now - req_time < timedelta(seconds=self.time_window)]
            
            if len(self.requests) >= self.max_requests:
                oldest = min(self.requests)
                sleep_time = (oldest + timedelta(seconds=self.time_window) - now).total_seconds()
                if sleep_time > 0:
                    print(f"Rate limit IUGU atingido. Aguardando {sleep_time:.2f} segundos...")
                    time.sleep(sleep_time)
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

def execute_curl(ssh_client, url, max_retries=5):
    for attempt in range(max_retries):
        try:
            if "?" in url:
                url += "&limit=50"
            else:
                url += "?limit=50"
                
            curl_cmd = f'curl -s -m 180 "{url}" -H "accept: application/json"'
            print(f"Tentativa {attempt + 1}/{max_retries}: Executando consulta...")
            
            stdin, stdout, stderr = ssh_client.exec_command(curl_cmd, timeout=180)
            error = stderr.read().decode('utf-8')
            response = stdout.read().decode('utf-8')
            
            if error:
                print(f"Erro no curl: {error}")
                sleep(10)
                continue
                
            if "error code: 504" in response:
                print("Erro 504 detectado, aguardando antes de tentar novamente...")
                sleep(15)
                continue
                
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                print(f"Erro ao decodificar JSON. Resposta: {response[:200]}...")
                sleep(10)
                continue
                
        except Exception as e:
            print(f"Erro na tentativa {attempt + 1}: {e}")
            sleep(10)
            
        if attempt < max_retries - 1:
            print("Tentando novamente após erro...")
            
    print(f"Todas as {max_retries} tentativas falharam para a URL: {url}")
    return None

def get_account_balance(ssh_client, token, account_id):
    try:
        # Verifica rate limit antes da requisição
        rate_limiter.wait_if_needed()
        
        # Faz apenas uma chamada com limit=1
        response = execute_curl(ssh_client, f"{url_financial}?api_token={token}&limit=1")
        
        if response and "transactions" in response:
            transactions = response.get("transactions", [])
            if transactions:
                last_transaction = transactions[0]
                saldo_cents = float(last_transaction["balance_cents"]) / 100
                total_transactions = response.get("total_items", 0)
                return {
                    "Account": account_id,
                    "transactions_total": total_transactions,
                    "saldo_cents": saldo_cents
                }
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
        wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas TESTE")

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
        
        # Processa contas em lotes pequenos para respeitar rate limit
        batch_size = 3  # Lotes menores para melhor controle
        total_contas = len(df_subcontas_ativas)
        
        for i in range(0, total_contas, batch_size):
            batch = df_subcontas_ativas.iloc[i:i+batch_size]
            batch_results = []
            
            print(f"\nProcessando lote {i//batch_size + 1}/{-(-total_contas//batch_size)}")
            
            for _, row in batch.iterrows():
                token = row["live_token_full"]
                account = row["account"]
                
                print(f"Account: {account}")
                resultado = get_account_balance(ssh_client, token, account)
                
                if resultado["saldo_cents"] > 0 or resultado["transactions_total"] > 0:
                    batch_results.append(resultado)
                    print(f"Resultado:")
                    print(f"- Saldo: R$ {resultado['saldo_cents']:,.2f}")
                    print(f"- Total transações: {resultado['transactions_total']}")
            
            resultados.extend(batch_results)
            
            # Pausa entre lotes para respeitar rate limit
            if i + batch_size < total_contas:
                sleep(2)
        
        # Cria DataFrame com resultados
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