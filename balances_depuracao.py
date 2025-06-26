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
            id AS merchant_id,
            name_text AS merchant_name,
            balance_decimal AS jaci_atual
        FROM public.core_merchant
        ORDER BY name_text;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["merchant_id", "merchant_name", "jaci_atual"])
        
        # Converte a coluna jaci_atual para numérico tratando casos especiais
        df['jaci_atual'] = df['jaci_atual'].apply(convert_to_numeric)
        
        print(f"Dados obtidos do PostgreSQL:")
        print(f"Total de merchants: {len(df)}")
        if not df.empty:
            print("Primeiros 5 registros:")
            for idx, row in df.head().iterrows():
                print(f"  ID: {row['merchant_id']}, Nome: {row['merchant_name']}, Saldo: {row['jaci_atual']}")
        
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

                print("\nAtualizando dados na aba 'jaci'...")
                df_jaci_atual = get_jaci_atual_from_postgres(cursor)
                if not df_jaci_atual.empty:
                    try:
                        print(f"Dados do PostgreSQL: {len(df_jaci_atual)} linhas")
                        print(f"Primeiros 5 registros:")
                        print(df_jaci_atual.head())
                        
                        # Prepara os dados para atualização completa da planilha
                        # Cria o DataFrame com as 3 colunas
                        df_to_update = df_jaci_atual[['merchant_name', 'merchant_id', 'jaci_atual']].copy()
                        df_to_update.columns = ['Merchant', 'Merchant_id', 'saldo_atual']
                        
                        # Arredonda saldo_atual para 2 casas decimais
                        df_to_update['saldo_atual'] = df_to_update['saldo_atual'].round(2)
                        
                        print(f"Dados preparados para atualização: {len(df_to_update)} registros")
                        print("Estrutura dos dados:")
                        print(df_to_update.head())
                        
                        # Limpa a planilha e adiciona os cabeçalhos + dados
                        wks_balances.clear()
                        
                        # Adiciona cabeçalhos
                        headers = ['Merchant', 'saldo_atual', 'Merchant_id']
                        wks_balances.update_row(1, headers)
                        
                        # Reorganiza as colunas para a ordem correta: Merchant, saldo_atual, Merchant_id
                        df_final = df_to_update[['Merchant', 'saldo_atual', 'Merchant_id']]
                        
                        # Adiciona os dados a partir da linha 2
                        wks_balances.set_dataframe(df_final, (2, 1), copy_head=False, encoding="utf-8")
                        
                        print(f"✓ Aba 'jaci' atualizada com sucesso com {len(df_final)} registros.")
                        print("✓ Estrutura: Coluna A=Merchant, Coluna B=saldo_atual, Coluna C=Merchant_id")
                            
                    except Exception as e:
                        print(f"Erro específico na atualização da aba jaci: {e}")
                        import traceback
                        print(traceback.format_exc())
                else:
                    print("⚠️ Nenhum dado retornado do PostgreSQL para a aba 'jaci'")

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
