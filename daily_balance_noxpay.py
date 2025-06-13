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

def safe_update_cell(sheet, cell_address, value):
    """Atualiza uma célula usando o método mais compatível disponível"""
    try:
        # Tenta primeiro com update_value (versões mais recentes)
        if hasattr(sheet, 'update_value'):
            sheet.update_value(cell_address, str(value))
            return True
        # Se não funcionar, tenta com update_acell (versões antigas)
        elif hasattr(sheet, 'update_acell'):
            sheet.update_acell(cell_address, str(value))
            return True
        # Se nada funcionar, usa update_values como alternativa
        elif hasattr(sheet, 'update_values'):
            # Converte A1 notation para row/col
            import re
            match = re.match(r'([A-Z]+)(\d+)', cell_address)
            if match:
                col_str, row_str = match.groups()
                # Converte coluna letra para número (A=1, B=2, etc.)
                col = 0
                for char in col_str:
                    col = col * 26 + (ord(char) - ord('A') + 1)
                row = int(row_str)
                
                sheet.update_values(f'{cell_address}:{cell_address}', [[str(value)]])
                return True
        
        print(f"❌ Nenhum método de atualização funcionou para {cell_address}")
        return False
        
    except Exception as e:
        print(f"❌ Erro ao atualizar célula {cell_address}: {e}")
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
        print("✓ Conexão com banco de dados estabelecida com sucesso.")
        return connection
    except Exception as e:
        print(f"❌ Erro ao conectar com o banco de dados: {e}")
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
            # Usa função robusta para atualizar células
            success1 = safe_update_cell(sheet, "E3", result[2])  # balance
            success2 = safe_update_cell(sheet, "B1", result[0])  # date_time na célula B1
            
            if success1 and success2:
                print(f"✓ Snapshot Transfeera atualizado: Balance={result[2]}, DateTime={result[0]}")
            else:
                print(f"⚠️ Snapshot Transfeera parcialmente atualizado: Balance={result[2]}, DateTime={result[0]}")
        else:
            print("⚠️ Nenhum dado encontrado para Transfeera")
    except Exception as e:
        print(f"❌ Erro ao obter snapshot Transfeera: {e}")
        import traceback
        print(traceback.format_exc())

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
            # Usa função robusta para atualizar células
            success = safe_update_cell(sheet, "F3", result[2])  # balance
            
            if success:
                print(f"✓ Snapshot Sqala atualizado: Balance={result[2]}, DateTime={result[0]}")
            else:
                print(f"⚠️ Falha ao atualizar Sqala: Balance={result[2]}, DateTime={result[0]}")
        else:
            print("⚠️ Nenhum dado encontrado para Sqala")
    except Exception as e:
        print(f"❌ Erro ao obter snapshot Sqala: {e}")
        import traceback
        print(traceback.format_exc())

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
            print(f"Processando {len(results)} registros de balances...")
            
            # Prepara os dados para atualização em lote
            ids_data = [[str(result[0])] for result in results]  # ids
            balances_data = [[str(result[1])] for result in results]  # balances
            
            try:
                # Tenta atualizar em lote (mais eficiente)
                print("Atualizando IDs na coluna A...")
                jaci_sheet.update_values(f'A1:A{len(ids_data)}', ids_data)
                
                print("Atualizando balances na coluna B...")
                jaci_sheet.update_values(f'B1:B{len(balances_data)}', balances_data)
                
                print(f"✓ Balances atualizados na página jaci: {len(results)} registros processados")
                
            except Exception as batch_error:
                print(f"⚠️ Erro na atualização em lote: {batch_error}")
                print("Tentando atualização célula por célula...")
                
                # Fallback: atualização célula por célula
                success_count = 0
                for i, result in enumerate(results[:100], 1):  # Limita a 100 para evitar timeout
                    id_success = safe_update_cell(jaci_sheet, f"A{i}", result[0])
                    balance_success = safe_update_cell(jaci_sheet, f"B{i}", result[1])
                    
                    if id_success and balance_success:
                        success_count += 1
                
                print(f"✓ Atualização individual concluída: {success_count}/{min(100, len(results))} registros")
            
        else:
            print("⚠️ Nenhum dado encontrado para balances")
            
    except Exception as e:
        print(f"❌ Erro ao obter balances: {e}")
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
            print("❌ Falha na conexão com o banco de dados")
            return False
        
        cursor = db_connection.cursor()
        
        # Conecta ao Google Sheets
        print("Conectando ao Google Sheets...")
        gc = pygsheets.authorize(service_file='controles.json')
        sh_balance = gc.open("Daily Balance - Nox Pay")
        
        print("Acessando abas do Google Sheets...")
        wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas")
        wks_jaci = sh_balance.worksheet_by_title("jaci")
        print("✓ Conexão com Google Sheets estabelecida!")
        
        # Executa as funções de snapshot
        print("\n--- Atualizando snapshots das contas ---")
        get_snapshot_transfeera(cursor, wks_IUGU_subacc)
        get_snapshot_sqala(cursor, wks_IUGU_subacc)
        
        # Executa a função de balances
        print("\n--- Atualizando balances na página jaci ---")
        get_balances(cursor, wks_jaci)
        
        print("\n✅ Todas as atualizações concluídas!")
        return True
        
    except Exception as e:
        print(f"❌ Erro durante verificação das contas: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    finally:
        # Fecha as conexões
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
            print("✓ Conexão com banco de dados fechada.")

def main():
    print("🚀 Iniciando Daily Balance NOX Pay...")
    
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    print("\nIniciando loop principal do Daily Balance...")
    while True:
        try:
            current_time = datetime.now(pytz.UTC).astimezone(pytz.timezone('America/Sao_Paulo'))
            print(f"\n{'='*60}")
            print(f"🕒 Atualização em: {current_time}")
            print(f"{'='*60}")

            # Executa a atualização das contas
            success = check_all_accounts()
            
            if success:
                consecutive_failures = 0
                print(f"\n✅ Atualização concluída com sucesso!")
            else:
                consecutive_failures += 1
                print(f"\n❌ Falha na atualização #{consecutive_failures}")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"❌ CRÍTICO: {max_consecutive_failures} falhas consecutivas. Encerrando...")
                    break

        except KeyboardInterrupt:
            print("\n⚠️ Interrupção pelo usuário. Encerrando...")
            break
        except Exception as e:
            consecutive_failures += 1
            print(f"\n❌ ERRO CRÍTICO #{consecutive_failures}: {e}")
            
            if consecutive_failures >= max_consecutive_failures:
                print(f"❌ CRÍTICO: {max_consecutive_failures} falhas consecutivas. Encerrando...")
                break
            
            print("Tentando reiniciar em 60 segundos...")
            import traceback
            print(traceback.format_exc())
            sleep(60)
            continue

        print(f"\n⏳ Aguardando 60 segundos para próxima atualização...")
        sleep(60)

if __name__ == "__main__":
    main()
