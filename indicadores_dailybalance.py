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
    query = """
    SELECT 
        subquery.merchant_id,
        cm.name_text AS merchant,
        AVG(subquery.contagem) AS media_pix_minuto
    FROM (
        SELECT 
            cp.merchant_id,
            DATE_TRUNC('minute', cp.created_at_date) AS minuto,
            COUNT(*) AS contagem
        FROM core_payment cp
        WHERE cp.status_text = 'PAID'
          AND cp.method_text IN ('PIX', 'PIXOUT')
          AND cp.created_at_date >= NOW() - INTERVAL '1 hour'
        GROUP BY cp.merchant_id, minuto
    ) subquery
    JOIN core_merchant cm ON subquery.merchant_id = cm.id
    GROUP BY subquery.merchant_id, cm.name_text
    ORDER BY media_pix_minuto DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)

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
      AND cp.created_at_date >= CURRENT_DATE AT TIME ZONE 'America/Sao_Paulo'
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
      AND cp.created_at_date >= CURRENT_DATE AT TIME ZONE 'America/Sao_Paulo'
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
      AND cp.created_at_date >= DATE_TRUNC('month', NOW() AT TIME ZONE 'America/Sao_Paulo')
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
    WHERE cp.created_at_date >= CURRENT_DATE AT TIME ZONE 'America/Sao_Paulo'
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
    WHERE cp.created_at_date >= CURRENT_DATE AT TIME ZONE 'America/Sao_Paulo'
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
    Obtém os pagamentos PIXOUT entre as datas fornecidas.
    """
    query = """
    SELECT
        cp.merchant_id,
        DATE_TRUNC('hour', cp.created_at_date AT TIME ZONE 'America/Sao_Paulo') AS data_hora,
        cm.name_text AS merchant, 
        cp.method_text AS method,
        COUNT(*) AS quantidade,
        SUM(cp.amount_decimal) AS volume
    FROM core_payment cp 
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.status_text = 'PAID'
      AND cp.method_text = 'PIXOUT'
      AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' BETWEEN %s AND %s
    GROUP BY cp.merchant_id, data_hora, merchant, method
    ORDER BY cp.merchant_id, data_hora, merchant;
    """
    cursor.execute(query, (start_date, end_date))
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=colnames)

############# MÉTRICAS DE SAQUES - ÚLTIMOS 30 DIAS #############
def get_withdrawal_metrics(cursor):
    """
    Calcula as estatísticas de saques (PIXOUT) nos últimos 30 dias.
    """
    end_date = datetime.now(TZ_SP)
    start_date = end_date - timedelta(days=30)

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
    last_1h = now - timedelta(hours=1)
    last_12h = now - timedelta(hours=12)
    last_24h = now - timedelta(hours=24)

    # Ajuste para usar created_at_date em vez de finalized_at_date
    query = """
    SELECT
        cp.merchant_id,
        cm.name_text AS merchant,
        SUM(CASE WHEN cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s THEN cp.amount_decimal ELSE 0 END) as last_1h,
        SUM(CASE WHEN cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s THEN cp.amount_decimal ELSE 0 END) as last_12h,
        SUM(CASE WHEN cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s THEN cp.amount_decimal ELSE 0 END) as last_24h
    FROM core_payment cp
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.status_text = 'PAID'
      AND cp.method_text = 'PIXOUT'
      AND cp.created_at_date AT TIME ZONE 'America/Sao_Paulo' >= %s
    GROUP BY cp.merchant_id, cm.name_text
    """
    cursor.execute(query, (last_1h, last_12h, last_24h, last_24h))
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=['merchant_id', 'merchant', 'current_1h_withdrawals', 'sum_12h_withdrawals', 'sum_24h_withdrawals'])
    return df

############# LOOP PRINCIPAL #############
def main():
    print("\nIniciando loop principal de indicadores...")
    while True:
        try:
            current_time = datetime.now(TZ_SP)
            print(f"\n{'='*50}")
            print(f"Nova atualização de indicadores iniciada em: {current_time}")
            print(f"{'='*50}")

            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    print("\nColetando métricas...")
                    df_pix = count_pix_transactions(cursor)
                    print("✓ Métricas PIX coletadas")
                    
                    df_daily_pix = count_daily_transactions(cursor)
                    print("✓ Métricas diárias coletadas")
                    
                    df_revenue = daily_revenue(cursor)
                    print("✓ Receita diária coletada")
                    
                    df_month_revenue = monthly_revenue(cursor)
                    print("✓ Receita mensal coletada")
                    
                    df_conversion = conversion_rate(cursor)
                    print("✓ Taxa de conversão calculada")
                    
                    df_fail = fail_rate(cursor)
                    print("✓ Taxa de falha calculada")
                    
                    df_withdrawal_metrics = get_withdrawal_metrics(cursor)
                    print("✓ Métricas de saque calculadas")
                    
                    df_recent_withdrawals = get_recent_withdrawals(cursor)
                    print("✓ Saques recentes coletados")

                    print("\nMesclando dados...")
                    # Mescla os DataFrames corretamente usando `merchant_id`
                    df_indicators = df_revenue.merge(df_pix, on=["merchant_id", "merchant"], how="left").fillna(0)
                    df_indicators = df_indicators.merge(df_daily_pix, on=["merchant_id", "merchant"], how="left").fillna(0)
                    df_indicators = df_indicators.merge(df_month_revenue, on=["merchant_id", "merchant"], how="outer", suffixes=('_daily', '_monthly'))
                    df_indicators = df_indicators.merge(df_conversion, on=["merchant_id", "merchant"], how="outer", suffixes=('', '_conv'))
                    df_indicators = df_indicators.merge(df_fail, on=["merchant_id", "merchant"], how="outer", suffixes=('', '_fail'))
                    df_indicators = df_indicators.merge(df_withdrawal_metrics, on=["merchant_id", "merchant"], how="left")
                    df_indicators = df_indicators.merge(df_recent_withdrawals,on=["merchant_id", "merchant"], how="left")
                    
                    print("\nAtualizando Google Sheets...")
                    # Antes de enviar para o Google Sheets
                    df_indicators = df_indicators.sort_values('volume', ascending=False)
                    df_indicators = df_indicators.drop_duplicates(subset=['merchant_id'])

                    # Formata os números para usar vírgula como decimal
                    for col in df_indicators.select_dtypes(include=['float64']).columns:
                        df_indicators[col] = df_indicators[col].apply(lambda x: '{:.2f}'.format(x).replace('.', ',') if pd.notnull(x) else 'NaN')

                    # Envia para o Google Sheets começando da linha 1
                    wks_ind.set_dataframe(df_indicators, (1, 1), encoding="utf-8", copy_head=True)
                    print("✓ Indicadores atualizados com sucesso")

                    print(f"Atualização concluída em: {datetime.now(TZ_SP)}")

        except Exception as e:
            print(f"\nERRO CRÍTICO: {e}")
            print("Fechando conexão antiga...")
            try:
                cursor.close()
                conn.close()
            except:
                pass
            print("Tentando reiniciar o loop em 60 segundos...")
            time.sleep(60)
            continue

        print("Aguardando 60 segundos para próxima atualização...")
        time.sleep(60)

if __name__ == "__main__":
    main()