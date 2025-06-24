import psycopg2
import time
import pygsheets
import pandas as pd
from datetime import datetime
import os
import json
from pathlib import Path
import numpy as np

############# CONFIGURAÇÃO DO GOOGLE SHEETS #############

try:
    print("Conectando ao Google Sheets...")
    gc = pygsheets.authorize(service_file=os.getenv('GOOGLE_SHEETS_CREDS', 'controles.json'))
    sh = gc.open('Daily Balance - Nox Pay')

    wks_JACI = sh.worksheet_by_title("DATABASE JACI")
    print("✓ Conectado à aba DATABASE JACI")

    wks_backtxs = sh.worksheet_by_title("Backoffice Ajustes")
    print("✓ Conectado à aba Backoffice Ajustes")

    wks_balances = sh.worksheet_by_title("jaci")
    print("✓ Conectado à aba jaci")

    print("Conexão com Google Sheets estabelecida com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao Google Sheets: {e}")
    raise

############# FUNÇÕES AUXILIARES #############

def get_last_row(worksheet):
    try:
        last_row = len(worksheet.get_col(9, include_tailing_empty=False)) + 1
        print(f"Última linha encontrada em {worksheet.title}: {last_row}")
        return last_row
    except Exception as e:
        print(f"Erro ao obter última linha: {e}")
        return 1

def convert_to_numeric(value):
    """Converte um valor para numérico, tratando casos especiais"""
    if value is None or pd.isna(value):
        return 0.0
    
    try:
        # Remove espaços e converte para string primeiro
        str_value = str(value).strip()
        
        # Se estiver vazio após strip, retorna 0
        if not str_value or str_value.lower() in ['', 'none', 'nan']:
            return 0.0
            
        # Tenta converter para float
        return float(str_value)
            
    except (ValueError, TypeError):
        print(f"Aviso: Não foi possível converter '{value}' para numérico. Usando 0.0.")
        return 0.0

############# FUNÇÕES DE CONSULTA AO BANCO #############

def get_balances(cursor):
    try:
        return
    except Exception as e:
        print(f"Erro ao obter saldos das contas: {e}")
        return

def get_payments(cursor):
    try:
        query = """
        SELECT DISTINCT
            DATE_TRUNC('day', cp.created_at_date AT TIME ZONE 'America/Sao_Paulo') AS data, 
            cm.name_text AS merchant, 
            cp.provider_text AS provider, 
            cp.method_text AS meth, 
            COUNT(*) AS quantidade, 
            SUM(cp.amount_decimal) AS volume
        FROM core_payment cp 
        JOIN core_merchant cm ON cm.id = cp.merchant_id
        WHERE cp.status_text = 'PAID' 
        AND cp.created_at_date >= (DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'GMT')
        GROUP BY data, merchant, cm.name_text, cp.provider_text, cp.method_text
        ORDER BY data DESC;
        """
        print("Executando query de pagamentos do dia...")
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["data", "merchant", "provider", "meth", "quantidade", "volume"])
        if not df.empty:
            df = df.drop_duplicates()
            print(f"✓ Query de pagamentos retornou {len(df)} registros do dia")
        return df
    except Exception as e:
        print(f"Erro ao obter pagamentos: {e}")
        return pd.DataFrame()

def get_backtransactions(cursor):
    try:
        query = """
        SELECT DISTINCT
            (SELECT cm2.name_text FROM core_merchant cm2 WHERE id = merchant_id) AS merchant,
            description_text AS descricao,
            SUM(amount_decimal) AS valor_total,
            DATE_TRUNC('minute', created_at_date AT TIME ZONE 'America/Sao_Paulo') AS data_criacao,
            MAX(created_at_date) as ultima_atualizacao
        FROM public.core_backofficetrasactions
        WHERE created_at_date >= (DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'GMT')
        GROUP BY DATE_TRUNC('minute', created_at_date AT TIME ZONE 'America/Sao_Paulo'), merchant_id, descricao
        ORDER BY ultima_atualizacao ASC
        LIMIT 100;
        """
        print("Executando query de backoffice do dia...")
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["merchant", "descricao", "valor_total", "data_criacao", "ultima_atualizacao"])
        if not df.empty:
            df['data_criacao'] = df['data_criacao'].dt.strftime('%Y-%m-%d %H:%M')
            df = df.drop(columns=["ultima_atualizacao"])
            df = df.drop_duplicates()
            print(f"✓ Query de backoffice retornou {len(df)} registros do dia")
        return df
    except Exception as e:
        print(f"Erro ao obter transações do backoffice: {e}")
        return pd.DataFrame()

def get_jaci_atual_from_postgres(cursor):
    try:
        query = """
        SELECT
            name_text AS merchant_name,
            balance_decimal AS jaci_atual
        FROM public.core_merchant
        ORDER BY name_text;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["merchant_name", "jaci_atual"])
        
        # Converte a coluna jaci_atual para numérico tratando casos especiais
        df['jaci_atual'] = df['jaci_atual'].apply(convert_to_numeric)
        
        return df
    except Exception as e:
        print(f"Erro ao buscar saldos atuais (Jaci Atual): {e}")
        return pd.DataFrame()

############# LOOP PRINCIPAL #############

print("\nIniciando loop principal...")
while True:
    try:
        current_time = datetime.now()
        print(f"\n{'='*50}")
        print(f"Nova atualização iniciada em: {current_time}")
        print(f"{'='*50}")

        if current_time.hour == 0 and current_time.minute == 0:
            print("Meia-noite detectada, aguardando 1 minuto...")
            time.sleep(60)

        print("\nConectando ao banco de dados...")
        with psycopg2.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'),
            port=int(os.getenv('DB_PORT', "5432"))
        ) as conn:
            print("✓ Conexão estabelecida com sucesso")

            with conn.cursor() as cursor:
                print("\nAtualizando saldos...")
                get_balances(cursor)

                print("\nAtualizando pagamentos...")
                df_payments = get_payments(cursor)
                if not df_payments.empty:
                    last_row_JACI = get_last_row(wks_JACI)
                    wks_JACI.set_dataframe(df_payments, (last_row_JACI, 1), encoding="utf-8", copy_head=False)
                    print("✓ Pagamentos atualizados com sucesso na aba 'DATABASE JACI'")

                print("\nAtualizando transações do backoffice...")
                df_backtxs = get_backtransactions(cursor)
                if not df_backtxs.empty:
                    last_row_backtxs = get_last_row(wks_backtxs)
                    wks_backtxs.set_dataframe(df_backtxs, (last_row_backtxs, 1), encoding="utf-8", copy_head=False)
                    print("✓ Transações do backoffice atualizadas com sucesso na aba 'Backoffice Ajustes'")

                print("\nAtualizando coluna 'saldo_atual' na aba 'jaci'...")
                df_jaci_atual = get_jaci_atual_from_postgres(cursor)
                if not df_jaci_atual.empty:
                    try:
                        sheet_data = wks_balances.get_all_records()
                        df_sheet = pd.DataFrame(sheet_data)
                        
                        print(f"Dados da planilha: {len(df_sheet)} linhas")
                        print(f"Dados do PostgreSQL: {len(df_jaci_atual)} linhas")

                        # Faz o merge dos dados
                        df_merge = pd.merge(df_sheet, df_jaci_atual, how='left', left_on='Merchant', right_on='merchant_name')
                        
                        # Prepara valores para atualização - CORRIGIDO
                        values_to_update = []
                        
                        for value in df_merge['jaci_atual']:
                            if pd.isna(value):
                                numeric_value = 0.0
                            else:
                                numeric_value = convert_to_numeric(value)
                            
                            # Arredonda para 2 casas decimais
                            rounded_value = round(numeric_value, 2)
                            values_to_update.append(rounded_value)
                        
                        print(f"Valores preparados para atualização: {len(values_to_update)} registros")
                        print(f"Primeiros 5 valores: {values_to_update[:5]}")
                        
                        # Atualiza a coluna (coluna 2 = B, assumindo que saldo_atual está na coluna B)
                        wks_balances.update_col(2, ['saldo_atual'] + values_to_update)
                        print("✓ Coluna 'saldo_atual' atualizada com sucesso na aba 'jaci'.")
                        
                    except Exception as e:
                        print(f"Erro específico na atualização da coluna saldo_atual: {e}")
                        import traceback
                        print(traceback.format_exc())
                else:
                    print("⚠️ Nenhum dado retornado do PostgreSQL para 'saldo_atual'")

    except Exception as e:
        print(f"\nERRO CRÍTICO: {e}")
        import traceback
        print(traceback.format_exc())
        try:
            cursor.close()
            conn.close()
        except:
            pass
        print("Tentando reiniciar o loop em 60 segundos...")
        time.sleep(60)
        continue

    print(f"\nAtualização concluída em: {datetime.now()}")
    print("Aguardando 60 segundos para próxima atualização...")
    time.sleep(60)
