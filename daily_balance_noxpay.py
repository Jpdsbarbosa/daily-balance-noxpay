import paramiko
import json
from time import sleep
import pandas as pd
from datetime import datetime
import pygsheets
import os

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

def get_account_balance(ssh_client, token, account):
    if not token or token.strip() == "":
        print(f"Token vazio para conta {account}, pulando...")
        return None

    max_retries = 5  # Número máximo de tentativas
    retry_delay = 10  # Tempo entre tentativas (segundos)
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"Tentativa {attempt + 1}/{max_retries} para conta {account}")
                sleep(retry_delay)  # Espera entre tentativas
            
            # Consulta saldo
            print(f"Consultando saldo da conta {account}...")
            command = f'curl -H "Authorization: Basic {token}" https://api.iugu.com/v1/balance'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            response = stdout.read().decode()
            
            if "504 Gateway Time-out" in response:
                print(f"Timeout na consulta de saldo, tentativa {attempt + 1}")
                continue  # Tenta novamente
                
            data = json.loads(response)
            
            if 'total_cents' not in data:
                print(f"Resposta inválida para saldo: {response}")
                continue  # Tenta novamente
            
            # Se chegou aqui, conseguiu o saldo. Agora busca transações
            sleep(2)  # Pequena pausa entre as chamadas
            
            command_trans = f'curl -H "Authorization: Basic {token}" "https://api.iugu.com/v1/financial_transaction_requests?limit=1"'
            stdin, stdout, stderr = ssh_client.exec_command(command_trans)
            trans_response = stdout.read().decode()
            
            if "504 Gateway Time-out" in trans_response:
                print("Timeout ao buscar transações, considerando 0")
                total_items = 0
            else:
                trans_data = json.loads(trans_response)
                total_items = trans_data.get('total_items', 0)
            
            # Se chegou até aqui, deu tudo certo
            return {
                "Account": account,
                "saldo_cents": data['total_cents'] / 100.0,
                "transactions_total": total_items
            }
            
        except Exception as e:
            print(f"Erro na tentativa {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:  # Última tentativa
                print(f"Todas as tentativas falharam para conta {account}")
                return None
    
    return None  # Retorna None se todas as tentativas falharem

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
        if not check_trigger(wks_IUGU_subacc):
            print("Trigger não está ativo (B1 = FALSE). Encerrando execução.")
            return

        print("Trigger ativo! Iniciando atualização...")
        update_status(wks_IUGU_subacc, "Atualizando...")

        # Lê as subcontas do Google Sheets (origem e destino)
        df_subcontas = pd.DataFrame(wks_subcontas.get_all_records())
        df_subcontas_ativas = df_subcontas[df_subcontas["NOX"] == "SIM"]
        
        # Lê as contas existentes na planilha de destino
        contas_destino = pd.DataFrame(wks_IUGU_subacc.get_all_records())
        contas_existentes = set()
        if not contas_destino.empty and 'Account' in contas_destino.columns:
            contas_existentes = set(contas_destino['Account'].astype(str))

        # Conecta ao SSH
        ssh_client = connect_ssh()
        
        # Lista para armazenar resultados
        resultados = []
        
        # Processa cada conta
        total_contas = len(df_subcontas_ativas)
        print(f"\nProcessando {total_contas} contas...")

        for idx, row in df_subcontas_ativas.iterrows():
            token = row["live_token_full"]
            account = str(row["account"])
            
            print(f"\nProcessando conta {idx + 1}/{total_contas}")
            print(f"Account: {account}")
            
            # Indica se é uma conta nova ou existente
            if account in contas_existentes:
                print(f"Conta {account} encontrada na planilha de destino, atualizando...")
            else:
                print(f"Nova conta ativa detectada: {account}, adicionando...")
            
            resultado = get_account_balance(ssh_client, token, account)
            
            if resultado:
                resultados.append(resultado)
                print(f"Resultado:")
                print(f"- Saldo: R$ {resultado['saldo_cents']:,.2f}")
                print(f"- Total transações: {resultado['transactions_total']}")
            else:
                print(f"Não foi possível obter dados para a conta {account}")
            
            # Pausa entre contas
            sleep(5)

        # Estatísticas finais
        contas_processadas = set(r['Account'] for r in resultados)
        contas_novas = contas_processadas - contas_existentes
        contas_atualizadas = contas_processadas & contas_existentes

        print("\n=== Estatísticas de Processamento ===")
        print(f"Total de contas processadas: {len(resultados)}")
        print(f"Contas novas adicionadas: {len(contas_novas)}")
        print(f"Contas existentes atualizadas: {len(contas_atualizadas)}")
        if contas_novas:
            print("\nNovas contas adicionadas:")
            for conta in contas_novas:
                print(f"- {conta}")

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
        
        print(f"\nExecução concluída: {rodado}")
        
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