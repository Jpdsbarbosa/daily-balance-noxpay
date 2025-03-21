import paramiko
import json
from time import sleep
import pandas as pd
from datetime import datetime
import pygsheets
import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# Configurações via variáveis de ambiente
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', "22"))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
url_financial = "https://api.iugu.com/v1/accounts/financial"

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
        max_retries = 3
        for attempt in range(max_retries):
            # Primeiro pega o total de transações
            response = execute_curl(ssh_client, f"{url_financial}?api_token={token}")
            
            if response and "transactions_total" in response:
                total_transactions = response["transactions_total"]
                
                # Pega apenas as últimas transações
                start = max(0, total_transactions - 50)
                response = execute_curl(ssh_client, f"{url_financial}?api_token={token}&start={start}")
                
                if response and response.get("transactions"):
                    last_transaction = response["transactions"][-1]
                    saldo_cents = float(last_transaction["balance_cents"]) / 100
                    return {
                        "Account": account_id,
                        "transactions_total": total_transactions,
                        "saldo_cents": saldo_cents
                    }
            
            if attempt < max_retries - 1:
                print(f"Tentativa {attempt + 1} falhou, aguardando 30 segundos...")
                sleep(30)
                
    except Exception as e:
        print(f"Erro ao processar conta {account_id}: {e}")
    
    print(f"Não foi possível obter saldo para a conta {account_id}")
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

def process_account(args):
    ssh_client, token, account = args
    try:
        print(f"\nProcessando conta: {account}")
        return get_account_balance(ssh_client, token, account)
    except Exception as e:
        print(f"Erro ao processar conta {account}: {e}")
        return None

def validate_account(ssh_client, token, account):
    """Valida se a conta está ativa e acessível"""
    try:
        response = execute_curl(ssh_client, f"{url_financial}?api_token={token}&limit=1")
        if response and "transactions_total" in response:
            print(f"Conta {account} validada com sucesso")
            return True
        return False
    except Exception as e:
        print(f"Erro ao validar conta {account}: {e}")
        return False

def check_all_accounts():
    try:
        print("\nIniciando conexão com Google Sheets...")
        gc = pygsheets.authorize(service_file="controles.json")
        sh_gateway = gc.open("Gateway")
        wks_subcontas = sh_gateway.worksheet_by_title("Subcontas")
        sh_balance = gc.open("Daily Balance - Nox Pay")
        wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas TESTE")

        if not check_trigger(wks_IUGU_subacc):
            print("Trigger não está ativo (B1 = FALSE). Encerrando execução.")
            return

        print("Trigger ativo! Iniciando atualização...")
        update_status(wks_IUGU_subacc, "Atualizando...")

        # Lê as subcontas do Google Sheets
        df_subcontas = pd.DataFrame(wks_subcontas.get_all_records())
        df_subcontas_ativas = df_subcontas[df_subcontas["NOX"] == "SIM"]

        # Conecta ao SSH para validação
        ssh_client = connect_ssh()
        
        # Valida as contas primeiro
        contas_validas = []
        print("\nValidando contas...")
        for idx, row in df_subcontas_ativas.iterrows():
            token = row["live_token_full"]
            account = row["account"]
            print(f"\nValidando conta {idx + 1}/{len(df_subcontas_ativas)}: {account}")
            
            if validate_account(ssh_client, token, account):
                contas_validas.append((token, account))
            sleep(1)  # Pequena pausa entre validações
        
        ssh_client.close()
        
        print(f"\nContas válidas: {len(contas_validas)} de {len(df_subcontas_ativas)}")

        # Cria pool de conexões SSH para processamento paralelo
        ssh_pool = []
        max_workers = 3  # Reduzido para 3 workers simultâneos
        for _ in range(max_workers):
            ssh_client = connect_ssh()
            ssh_pool.append(ssh_client)

        resultados = []
        total_contas = len(contas_validas)
        contas_processadas = 0

        # Processa apenas as contas válidas em paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            # Processa em lotes menores
            batch_size = 5
            for i in range(0, len(contas_validas), batch_size):
                batch = contas_validas[i:i + batch_size]
                
                # Prepara argumentos do lote
                batch_args = [
                    (ssh_pool[j % max_workers], token, account)
                    for j, (token, account) in enumerate(batch)
                ]
                
                # Submete o lote
                batch_futures = [executor.submit(process_account, args) for args in batch_args]
                futures.extend(batch_futures)
                
                # Processa resultados do lote
                for future in concurrent.futures.as_completed(batch_futures):
                    resultado = future.result()
                    if resultado:
                        resultados.append(resultado)
                        contas_processadas += 1
                        print(f"Progresso: {contas_processadas}/{total_contas}")
                
                # Pausa entre lotes
                if i + batch_size < len(contas_validas):
                    print("\nPausa entre lotes...")
                    sleep(3)

        # Atualiza planilha
        if resultados:
            df_resultados = pd.DataFrame(resultados)
            df_resultados = df_resultados[["Account", "transactions_total", "saldo_cents"]]
            
            rodado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            wks_IUGU_subacc.update_value("A1", f"Última atualização: {rodado}")
            
            wks_IUGU_subacc.set_dataframe(
                df_resultados, 
                (2,1), 
                encoding='utf-8', 
                copy_head=True
            )

        print("\n=== Resumo da Execução ===")
        print(f"Total de contas NOX: {len(df_subcontas_ativas)}")
        print(f"Contas válidas: {len(contas_validas)}")
        print(f"Contas processadas com sucesso: {len(resultados)}")
        print(f"Execução concluída: {rodado}")
        
        reset_trigger(wks_IUGU_subacc)
        
        # Fecha conexões SSH
        for ssh_client in ssh_pool:
            ssh_client.close()

    except Exception as e:
        print(f"Erro durante a execução: {e}")
        import traceback
        print(traceback.format_exc())
        if 'ssh_pool' in locals():
            for ssh_client in ssh_pool:
                try:
                    ssh_client.close()
                except:
                    pass

if __name__ == "__main__":
    print("Iniciando script...")
    print("Verificando variáveis de ambiente...")
    for env_var in ['SSH_HOST', 'SSH_PORT', 'SSH_USERNAME', 'SSH_PASSWORD']:
        print(f"- {env_var}: {'✓' if os.getenv(env_var) else '✗'}")
    
    try:
        check_all_accounts()
    except Exception as e:
        print(f"Erro fatal: {e}")
        import traceback
        print(traceback.format_exc())