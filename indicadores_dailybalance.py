import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import pygsheets
import os
import pytz
import time

############# CONFIGURAÇÃO DO GOOGLE SHEETS #############
gc = pygsheets.authorize(service_account_env_var="GOOGLE_CREDENTIALS")  # Alterado para usar variável de ambiente
sh = gc.open('Daily Balance - Nox Pay')

# Página onde os indicadores serão escritos
wks_ind = sh.worksheet_by_title("indicadores")

# Configurações do Banco de Dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT', "5432"))
}

# Configuração do fuso horário
TZ_SP = pytz.timezone('America/Sao_Paulo')

############# CONSULTAS SQL AJUSTADAS PARA INCLUIR MERCHANT_ID #############

def count_pix_transactions(cursor):
    """
    Versão otimizada da consulta de transações PIX
    """
    try:
        # Aumenta o timeout para 2 minutos
        cursor.execute("SET statement_timeout = '120s'")
        
        query = """
        WITH last_hour_transactions AS (
            SELECT 
                cp.merchant_id,
                DATE_TRUNC('minute', cp.created_at_date AT TIME ZONE 'America/Sao_Paulo') AS minuto,
                COUNT(*) AS contagem
            FROM core_payment cp
            WHERE cp.status_text = 'PAID'
              AND cp.method_text IN ('PIX', 'PIXOUT')
              AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= (NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '1 hour')
            GROUP BY cp.merchant_id, minuto
        )
        SELECT 
            t.merchant_id,
            cm.name_text AS merchant,
            COALESCE(AVG(t.contagem), 0) AS media_pix_minuto
        FROM last_hour_transactions t
        JOIN core_merchant cm ON t.merchant_id = cm.id
        GROUP BY t.merchant_id, cm.name_text
        ORDER BY media_pix_minuto DESC;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        return pd.DataFrame(results, columns=colnames)
    except Exception as e:
        print(f"Erro em count_pix_transactions: {e}")
        return pd.DataFrame(columns=['merchant_id', 'merchant', 'media_pix_minuto'])

def count_daily_transactions(cursor):
    query = """
    SELECT 
        cp.merchant_id,
        cm.name_text AS merchant,
        COUNT(*) AS quantidade_pix_dia
    FROM core_payment cp
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.status_text = 'PAID'
      AND cp.method_text IN ('PIX', 'PIXOUT')
      AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= 
          DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo')
    GROUP BY cp.merchant_id, cm.name_text
    ORDER BY quantidade_pix_dia DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)

def daily_revenue(cursor):
    query = """
    SELECT 
        cp.merchant_id,
        cm.name_text AS merchant, 
        SUM(cp.amount_decimal) AS volume
    FROM core_payment cp
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.status_text = 'PAID'
      AND cp.method_text = 'FEE'
      AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= 
          DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo')
    GROUP BY cp.merchant_id, cm.name_text
    ORDER BY volume DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)

def monthly_revenue(cursor):
    query = """
    SELECT 
        cp.merchant_id,
        cm.name_text AS merchant, 
        SUM(cp.amount_decimal) AS volume_mensal
    FROM core_payment cp
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.status_text = 'PAID'
      AND cp.method_text = 'FEE'
      AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= 
          DATE_TRUNC('month', NOW() AT TIME ZONE 'America/Sao_Paulo')
    GROUP BY cp.merchant_id, cm.name_text
    ORDER BY volume_mensal DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)

def conversion_rate(cursor):
    query = """
    SELECT 
        cp.merchant_id,
        cm.name_text AS merchant,
        COUNT(CASE WHEN cp.status_text = 'PAID' THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0) AS taxa_conversao
    FROM core_payment cp
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= 
          DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo')
    GROUP BY cp.merchant_id, cm.name_text
    ORDER BY taxa_conversao DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)

def fail_rate(cursor):
    query = """
    SELECT
        cp.merchant_id,
        cm.name_text AS merchant,
        COUNT(CASE WHEN cp.status_text = 'FAIL' THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0) AS taxa_falha
    FROM core_payment cp
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= 
          DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo')
    GROUP BY cp.merchant_id, cm.name_text
    ORDER BY taxa_falha DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)
    

############# CONSULTA DE PAGAMENTOS (PIXOUT) PARA INDICADORES #############
def get_withdrawals(cursor, start_date, end_date):
    """
    Versão otimizada da consulta de saques
    """
    try:
        # Aumenta o timeout para 2 minutos
        cursor.execute("SET statement_timeout = '120s'")
        cursor.execute("SET work_mem = '256MB'")  # Aumenta a memória de trabalho
        
        query = """
        WITH hourly_withdrawals AS (
            SELECT
                cp.merchant_id,
                DATE_TRUNC('hour', cp.created_at_date AT TIME ZONE 'America/Sao_Paulo') AS data_hora,
                COUNT(*) AS quantidade,
                SUM(cp.amount_decimal) AS volume
            FROM core_payment cp
            WHERE cp.status_text = 'PAID'
              AND cp.method_text = 'PIXOUT'
              AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' BETWEEN %s AND %s
            GROUP BY cp.merchant_id, data_hora
        )
        SELECT
            hw.merchant_id,
            hw.data_hora,
            cm.name_text AS merchant,
            'PIXOUT' AS method,
            hw.quantidade,
            hw.volume
        FROM hourly_withdrawals hw
        JOIN core_merchant cm ON hw.merchant_id = cm.id
        ORDER BY hw.merchant_id, hw.data_hora;
        """
        
        cursor.execute(query, (start_date, end_date))
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        return pd.DataFrame(results, columns=colnames)
    except Exception as e:
        print(f"Erro ao buscar saques: {e}")
        return pd.DataFrame(columns=['merchant_id', 'data_hora', 'merchant', 'method', 'quantidade', 'volume'])

############# MÉTRICAS DE SAQUES - ÚLTIMOS 30 DIAS #############
def get_withdrawal_metrics(cursor):
    """
    Calcula as estatísticas de saques (PIXOUT) nos últimos 30 dias.
    """
    end_date = datetime.now(TZ_SP)
    start_date = end_date - timedelta(days=30)
    
    # Converte as datas para o timezone correto
    start_date = start_date.astimezone(TZ_SP)
    end_date = end_date.astimezone(TZ_SP)

    df = get_withdrawals(cursor, start_date, end_date)

    if df.empty:
        return pd.DataFrame(columns=[
            "merchant_id", "merchant", "mean_1h_volume", "std_1h_volume", "mean_1h_quantidade", "std_1h_quantidade",
            "mean_12h_volume", "std_12h_volume", "mean_12h_quantidade", "std_12h_quantidade",
            "mean_1d_volume", "std_1d_volume", "mean_1d_quantidade", "std_1d_quantidade"
        ])

    df["data_hora"] = pd.to_datetime(df["data_hora"])

    # Preenche os períodos vazios com 0
    idx = pd.date_range(start=start_date, end=end_date, freq='H')
    
    # Cálculo correto das métricas por período
    metrics = []
    for merchant_id, merchant_df in df.groupby(["merchant_id", "merchant"]):
        # Reindexação com preenchimento de zeros para períodos sem dados
        merchant_ts = merchant_df.set_index('data_hora').reindex(idx, fill_value=0)
        
        # Cálculos para 1h
        h1_stats = merchant_ts[['volume', 'quantidade']].resample('1H').sum().agg(['mean', 'std'])
        
        # Cálculos para 12h
        h12_stats = merchant_ts[['volume', 'quantidade']].resample('12H').sum().agg(['mean', 'std'])
        
        # Cálculos para 24h
        d1_stats = merchant_ts[['volume', 'quantidade']].resample('24H').sum().agg(['mean', 'std'])
        
        metrics.append({
            'merchant_id': merchant_id[0],
            'merchant': merchant_id[1],
            'mean_1h_volume': h1_stats['volume']['mean'],
            'std_1h_volume': h1_stats['volume']['std'],
            'mean_1h_quantidade': h1_stats['quantidade']['mean'],
            'std_1h_quantidade': h1_stats['quantidade']['std'],
            'mean_12h_volume': h12_stats['volume']['mean'],
            'std_12h_volume': h12_stats['volume']['std'],
            'mean_12h_quantidade': h12_stats['quantidade']['mean'],
            'std_12h_quantidade': h12_stats['quantidade']['std'],
            'mean_1d_volume': d1_stats['volume']['mean'],
            'std_1d_volume': d1_stats['volume']['std'],
            'mean_1d_quantidade': d1_stats['quantidade']['mean'],
            'std_1d_quantidade': d1_stats['quantidade']['std']
        })
    
    return pd.DataFrame(metrics)

############# SAQUES NA ÚLTIMA 1H, 12H, 24H #############
def get_recent_withdrawals(cursor):
    """
    Obtém os saques dos últimos 1h, 12h e 24h.
    """
    now = datetime.now(TZ_SP)
    last_1h = (now - timedelta(hours=1)).astimezone(TZ_SP)
    last_12h = (now - timedelta(hours=12)).astimezone(TZ_SP)
    last_24h = (now - timedelta(hours=24)).astimezone(TZ_SP)

    query = """
    WITH recent_withdrawals AS (
        SELECT
            cp.merchant_id,
            cm.name_text AS merchant,
            cp.created_at_date,
            cp.amount_decimal
        FROM core_payment cp
        JOIN core_merchant cm ON cm.id = cp.merchant_id
        WHERE cp.status_text = 'PAID'
          AND cp.method_text = 'PIXOUT'
          AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s
    )
    SELECT
        merchant_id,
        merchant,
        SUM(CASE WHEN created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s THEN amount_decimal ELSE 0 END) as current_1h_withdrawals,
        SUM(CASE WHEN created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s THEN amount_decimal ELSE 0 END) as sum_12h_withdrawals,
        SUM(amount_decimal) as sum_24h_withdrawals
    FROM recent_withdrawals
    GROUP BY merchant_id, merchant
    """
    
    try:
        # Adiciona configuração de timeout e isolation level
        cursor.execute("SET statement_timeout = '30s'")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.execute(query, (last_24h, last_1h, last_12h))
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=['merchant_id', 'merchant', 'current_1h_withdrawals', 'sum_12h_withdrawals', 'sum_24h_withdrawals'])
    except Exception as e:
        print(f"Erro ao buscar saques recentes: {e}")
        return pd.DataFrame(columns=['merchant_id', 'merchant', 'current_1h_withdrawals', 'sum_12h_withdrawals', 'sum_24h_withdrawals'])

############# LOOP PRINCIPAL #############
def execute_with_retry(cursor, query, params=None, max_retries=3, delay=5):
    """
    Executa uma query com tentativas em caso de erro
    """
    for attempt in range(max_retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=colnames)
        except Exception as e:
            print(f"Tentativa {attempt + 1} falhou: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                # Tenta recriar a conexão
                try:
                    cursor.close()
                    conn = psycopg2.connect(
                        **DB_CONFIG,
                        application_name='indicadores_dailybalance',
                        options='-c statement_timeout=120s -c work_mem=256MB'
                    )
                    conn.set_session(autocommit=True)
                    cursor = conn.cursor()
                except:
                    pass
            else:
                return pd.DataFrame()  # Retorna DataFrame vazio se todas as tentativas falharem

def main():
    print("\nIniciando loop principal de indicadores...")
    while True:
        conn = None
        cursor = None
        try:
            current_time = datetime.now(TZ_SP)
            print(f"\n{'='*50}")
            print(f"Nova atualização de indicadores iniciada em: {current_time}")
            print(f"{'='*50}")

            # Configurações otimizadas para a conexão
            conn = psycopg2.connect(
                **DB_CONFIG,
                application_name='indicadores_dailybalance',
                options='-c statement_timeout=120s -c work_mem=256MB'
            )
            conn.set_session(autocommit=True)
            cursor = conn.cursor()

            print("\nColetando métricas...")
            
            # Coleta as métricas com tratamento de erro individual
            results = {}
            
            # Lista de funções e seus nomes
            metrics_functions = [
                (count_pix_transactions, "Métricas PIX"),
                (count_daily_transactions, "Métricas diárias"),
                (daily_revenue, "Receita diária"),
                (monthly_revenue, "Receita mensal"),
                (conversion_rate, "Taxa de conversão"),
                (fail_rate, "Taxa de falha")
            ]

            # Coleta cada métrica com nova conexão para cada uma
            for func, name in metrics_functions:
                try:
                    # Cria nova conexão para cada métrica
                    with psycopg2.connect(**DB_CONFIG) as conn_metric:
                        with conn_metric.cursor() as cursor_metric:
                            results[name] = func(cursor_metric)
                            print(f"✓ {name} coletadas")
                except Exception as e:
                    print(f"Erro ao coletar {name}: {e}")
                    results[name] = pd.DataFrame()

            # Coleta métricas de saque com nova conexão
            try:
                with psycopg2.connect(**DB_CONFIG) as conn_withdrawal:
                    with conn_withdrawal.cursor() as cursor_withdrawal:
                        df_withdrawal_metrics = get_withdrawal_metrics(cursor_withdrawal)
                        print("✓ Métricas de saque calculadas")
            except Exception as e:
                print(f"Erro ao coletar métricas de saque: {e}")
                df_withdrawal_metrics = pd.DataFrame()

            # Coleta saques recentes com nova conexão
            try:
                with psycopg2.connect(**DB_CONFIG) as conn_recent:
                    with conn_recent.cursor() as cursor_recent:
                        df_recent_withdrawals = get_recent_withdrawals(cursor_recent)
                        print("✓ Saques recentes coletados")
            except Exception as e:
                print(f"Erro ao coletar saques recentes: {e}")
                df_recent_withdrawals = pd.DataFrame()

            print("\nMesclando dados...")
            
            # Verifica se temos os DataFrames necessários antes de mesclar
            if not results.get('Receita diária').empty and not results.get('Métricas PIX').empty:
                df_indicators = results['Receita diária'].merge(
                    results['Métricas PIX'], 
                    on=["merchant_id", "merchant"], 
                    how="left"
                ).fillna(0)
                
                # Continua mesclando apenas se tivermos dados
                for name, df in results.items():
                    if name not in ['Receita diária', 'Métricas PIX'] and not df.empty:
                        df_indicators = df_indicators.merge(
                            df,
                            on=["merchant_id", "merchant"],
                            how="left"
                        ).fillna(0)

                if not df_withdrawal_metrics.empty:
                    df_indicators = df_indicators.merge(
                        df_withdrawal_metrics,
                        on=["merchant_id", "merchant"],
                        how="left"
                    ).fillna(0)

                if not df_recent_withdrawals.empty:
                    df_indicators = df_indicators.merge(
                        df_recent_withdrawals,
                        on=["merchant_id", "merchant"],
                        how="left"
                    ).fillna(0)

                print("\nAtualizando Google Sheets...")
                df_indicators = df_indicators.sort_values('volume', ascending=False)
                df_indicators = df_indicators.drop_duplicates(subset=['merchant_id'])

                # Formata os números
                for col in df_indicators.select_dtypes(include=['float64']).columns:
                    df_indicators[col] = df_indicators[col].apply(
                        lambda x: '{:.2f}'.format(x).replace('.', ',') if pd.notnull(x) else 'NaN'
                    )

                # Atualiza o Google Sheets
                wks_ind.set_dataframe(df_indicators, (1, 1), encoding="utf-8", copy_head=True)
                print("✓ Indicadores atualizados com sucesso")
            else:
                print("Erro: Dados básicos não disponíveis para mesclagem")

            print(f"Atualização concluída em: {datetime.now(TZ_SP)}")

        except Exception as e:
            print(f"\nERRO CRÍTICO: {e}")
            print("Fechando conexão antiga...")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            print("Tentando reiniciar em 180 segundos...")
            time.sleep(180)
            continue
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        print("Aguardando 180 segundos para próxima atualização...")
        time.sleep(180)

if __name__ == "__main__":
    main()