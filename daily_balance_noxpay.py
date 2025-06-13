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
            return
        
        cursor = db_connection.cursor()
        
        # Conecta ao Google Sheets
        gc = pygsheets.authorize(service_file="GOOGLE_SHEETS_CREDS")
        sh_balance = gc.open("Daily Balance - Nox Pay")
        wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas")
        wks_jaci = sh_balance.worksheet_by_title("jaci")  # Adicionado para a função get_balances
        
        # Executa as funções de snapshot
        print("Atualizando snapshots das contas...")
        get_snapshot_transfeera(cursor, wks_IUGU_subacc)
        get_snapshot_sqala(cursor, wks_IUGU_subacc)
        
        # Executa a função de balances
        print("Atualizando balances na página jaci...")
        get_balances(cursor, wks_jaci)
        
        print("Todas as atualizações concluídas com sucesso!")
        
    except Exception as e:
        print(f"Erro durante verificação das contas: {e}")
        import traceback
        print(traceback.format_exc())
    finally:
        # Fecha as conexões
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
            print("Conexão com banco de dados fechada.")

def main():
    print("\nIniciando loop principal do Daily Balance...")
    while True:
        try:
            current_time = datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))
            print(f"\n{'='*50}")
            print(f"Atualização em: {current_time}")
            print(f"{'='*50}")

            # Executa a atualização das contas
            print("Iniciando atualização dos snapshots...")
            check_all_accounts()

        except Exception as e:
            print(f"\nERRO CRÍTICO: {e}")
            print("Tentando reiniciar o loop em 60 segundos...")
            import traceback
            print(traceback.format_exc())
            sleep(60)
            continue

        print(f"\nAtualização concluída em: {datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))}")
        print("Aguardando 60 segundos para próxima atualização...")
        sleep(60)  # Executa a cada 1 minuto

if __name__ == "__main__":
    print("Iniciando Daily Balance NOX Pay...")
    main()
