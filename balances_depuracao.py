import paramiko
import json
from time import sleep
from datetime import datetime
import pygsheets
import os
import pytz
import psycopg2

# Configurações do banco de dados PostgreSQL
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', "5432"))
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

def validate_credentials_file():
    """Valida se o arquivo de credenciais existe e é válido"""
    creds_file = os.getenv('GOOGLE_SHEETS_CREDS', 'controles.json')
    
    if not os.path.exists(creds_file):
        print(f"ERRO: Arquivo de credenciais não encontrado: {creds_file}")
        return False
    
    try:
        with open(creds_file, 'r') as f:
            content = f.read().strip()
            if not content:
                print(f"ERRO: Arquivo de credenciais está vazio: {creds_file}")
                return False
            
            # Tenta fazer parse do JSON
            json.loads(content)
            print(f"✓ Arquivo de credenciais válido: {creds_file}")
            return True
            
    except json.JSONDecodeError as e:
        print(f"ERRO: Arquivo de credenciais com formato JSON inválido: {e}")
        return False
    except Exception as e:
        print(f"ERRO: Não foi possível ler o arquivo de credenciais: {e}")
        return False

def connect_database():
    """Conecta ao banco de dados PostgreSQL"""
    try:
        print("Conectando ao banco de dados PostgreSQL...")
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conexão com banco de dados estabelecida com sucesso.")
        return connection
    except Exception as e:
        print(f"Erro ao conectar com o banco de dados: {e}")
        return None

def connect_google_sheets():
    """Conecta ao Google Sheets com validação de credenciais"""
    try:
        # Valida o arquivo de credenciais primeiro
        if not validate_credentials_file():
            return None, None, None
        
        print("Conectando ao Google Sheets...")
        creds_file = os.getenv('GOOGLE_SHEETS_CREDS', 'controles.json')
        gc = pygsheets.authorize(service_file=creds_file)
        
        sh_balance = gc.open("Daily Balance - Nox Pay")
        wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas")
        wks_jaci = sh_balance.worksheet_by_title("jaci")
        
        print("✓ Conexão com Google Sheets estabelecida com sucesso!")
        return gc, wks_IUGU_subacc, wks_jaci
        
    except Exception as e:
        print(f"Erro ao conectar com Google Sheets: {e}")
        import traceback
        print(traceback.format_exc())
        return None, None, None

def get_snapshot_transfeera(cursor, sheet):
    """Obtém o snapshot mais recente da conta Transfeera"""
    try:
        cursor.execute("""
            SELECT  
                DATE_TRUNC('minute', date_time - INTERVAL '3 hours') AS date_time,
                account_bank_text,
                balance AS min_balance
            FROM public.core_bankbalance
            WHERE account_bank_text = 'transfeera'
            ORDER BY date_time DESC
            LIMIT 1;
        """)
        result = cursor.fetchone()
        if result:
            sheet.update_acell("E3", str(result[2]))  # balance
            sheet.update_acell("B1", str(result[0]))  # date_time na célula B1
            print(f"Snapshot Transfeera atualizado: Balance={result[2]}, DateTime={result[0]}")
        else:
            print("Nenhum dado encontrado para Transfeera")
    except Exception as e:
        print(f"Erro ao obter snapshot Transfeera: {e}")

def get_snapshot_sqala(cursor, sheet):
    """Obtém o snapshot mais recente da conta Sqala"""
    try:
        cursor.execute("""
            SELECT  
                DATE_TRUNC('minute', date_time - INTERVAL '3 hours') AS date_time,
                account_bank_text,
                balance AS min_balance
            FROM public.core_bankbalance
            WHERE account_bank_text = 'sqala'
            ORDER BY date_time DESC
            LIMIT 1;
        """)
        result = cursor.fetchone()
        if result:
            sheet.update_acell("F3", str(result[2]))  # balance
            print(f"Snapshot Sqala atualizado: Balance={result[2]}, DateTime={result[0]}")
        else:
            print("Nenhum dado encontrado para Sqala")
    except Exception as e:
        print(f"Erro ao obter snapshot Sqala: {e}")

def get_balances(cursor, jaci_sheet):
    """Obtém os balances dos merchants e atualiza a página jaci"""
    try:
        cursor.execute("""
            SELECT
                id AS id,
                min(balance_decimal) AS "MIN(balance_decimal)"
            FROM public.core_merchant
            GROUP BY id
            ORDER BY id ASC
            LIMIT 1000
        """)
        results = cursor.fetchall()
        
        if results:
            # Prepara os dados para atualização em lote
            ids = []
            balances = []
            
            for result in results:
                ids.append(str(result[0]))  # id
                balances.append(str(result[1]))  # min balance
            
            # Limpa as colunas A e B primeiro (opcional, para garantir dados limpos)
            print("Limpando dados anteriores da página jaci...")
            jaci_sheet.update_values('A:A', [['']] * 1000)  # Limpa coluna A
            jaci_sheet.update_values('B:B', [['']] * 1000)  # Limpa coluna B
            
            # Atualiza coluna A com os IDs
            print("Atualizando coluna A com IDs...")
            ids_range = f'A1:A{len(ids)}'
            jaci_sheet.update_values(ids_range, [[id_val] for id_val in ids])
            
            # Atualiza coluna B com os balances
            print("Atualizando coluna B com balances...")
            balances_range = f'B1:B{len(balances)}'
            jaci_sheet.update_values(balances_range, [[balance] for balance in balances])
            
            print(f"Balances atualizados na página jaci: {len(results)} registros processados")
            
        else:
            print("Nenhum dado encontrado para balances")
            
    except Exception as e:
        print(f"Erro ao obter balances: {e}")
        import traceback
        print(traceback.format_exc())

def check_all_accounts():
    """Função principal para verificar todas as contas"""
    db_connection = None
    cursor = None
    
    try:
        # Conecta ao banco de dados
        db_connection = connect_database()
        if not db_connection:
            print("Falha na conexão com o banco de dados")
            return False
        
        cursor = db_connection.cursor()
        
        # Conecta ao Google Sheets com validação
        gc, wks_IUGU_subacc, wks_jaci = connect_google_sheets()
        if not gc or not wks_IUGU_subacc or not wks_jaci:
            print("Falha na conexão com Google Sheets")
            return False
        
        # Executa as funções de snapshot
        print("Atualizando snapshots das contas...")
        get_snapshot_transfeera(cursor, wks_IUGU_subacc)
        get_snapshot_sqala(cursor, wks_IUGU_subacc)
        
        # Executa a função de balances
        print("Atualizando balances na página jaci...")
        get_balances(cursor, wks_jaci)
        
        print("Todas as atualizações concluídas com sucesso!")
        return True
        
    except Exception as e:
        print(f"Erro durante verificação das contas: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    finally:
        # Fecha as conexões
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
            print("Conexão com banco de dados fechada.")

def check_environment_variables():
    """Verifica se todas as variáveis de ambiente necessárias estão definidas"""
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASS']
    optional_vars = ['DB_PORT', 'GOOGLE_SHEETS_CREDS']
    
    print("Verificando variáveis de ambiente...")
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            print(f"✓ {var}: Definida")
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"✓ {var}: {value}")
        else:
            print(f"⚠️ {var}: Não definida (usando padrão)")
    
    if missing_vars:
        print(f"❌ ERRO: Variáveis de ambiente obrigatórias não definidas: {missing_vars}")
        return False
    
    return True

def main():
    print("Iniciando Daily Balance NOX Pay...")
    
    # Verifica variáveis de ambiente
    if not check_environment_variables():
        print("❌ Falha na verificação das variáveis de ambiente. Encerrando...")
        return
    
    print("\nIniciando loop principal do Daily Balance...")
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    while True:
        try:
            current_time = datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))
            print(f"\n{'='*50}")
            print(f"Atualização em: {current_time}")
            print(f"{'='*50}")

            # Executa a atualização das contas
            print("Iniciando atualização dos snapshots...")
            success = check_all_accounts()
            
            if success:
                consecutive_failures = 0
                print(f"\n✓ Atualização concluída com sucesso em: {datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))}")
            else:
                consecutive_failures += 1
                print(f"\n❌ Falha na atualização #{consecutive_failures}")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"❌ ERRO CRÍTICO: {max_consecutive_failures} falhas consecutivas. Encerrando aplicação...")
                    break

        except KeyboardInterrupt:
            print("\n⚠️ Interrupção pelo usuário. Encerrando...")
            break
        except Exception as e:
            consecutive_failures += 1
            print(f"\n❌ ERRO CRÍTICO #{consecutive_failures}: {e}")
            print("Tentando reiniciar o loop em 60 segundos...")
            import traceback
            print(traceback.format_exc())
            
            if consecutive_failures >= max_consecutive_failures:
                print(f"❌ ERRO CRÍTICO: {max_consecutive_failures} falhas consecutivas. Encerrando aplicação...")
                break
            
            sleep(60)
            continue

        print("Aguardando 60 segundos para próxima atualização...")
        sleep(60)  # Executa a cada 1 minuto

if __name__ == "__main__":
    main()
