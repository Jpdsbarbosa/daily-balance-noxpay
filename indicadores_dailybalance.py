import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import pygsheets
import os
import pytz

############# CONFIGURAÇÃO DO GOOGLE SHEETS #############
gc = pygsheets.authorize(service_account_env_var="GOOGLE_CREDENTIALS")  # Alterado para usar variável de ambiente
sh = gc.open('Daily Balance - Nox Pay')

# Página onde os indicadores serão escritos
wks_ind = sh.worksheet_by_title("indicadores TESTE")

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

############# VERIFICAR ATIVAÇÃO NO GOOGLE SHEETS #############
def check_trigger():
    """Verifica se a célula B1 contém TRUE para executar o script."""
    status = wks_ind.get_value("B1")
    return status.strip().upper() == "TRUE"

def reset_trigger():
    """Após a execução, redefine a célula B1 para FALSE."""
    wks_ind.update_value("B1", "FALSE")

def update_status(status):
    """Atualiza o status de execução na célula A1."""
    wks_ind.update_value("A1", status)

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
        DATE_TRUNC('hour', cp.finalized_at_date AT TIME ZONE 'America/Sao_Paulo') AS data_hora,
        cm.name_text AS merchant, 
        cp.method_text AS method,
        COUNT(*) AS quantidade,
        SUM(cp.amount_decimal) AS volume
    FROM core_payment cp 
    JOIN core_merchant cm ON cm.id = cp.merchant_id
    WHERE cp.status_text = 'PAID'
      AND cp.method_text = 'PIXOUT'
      AND cp.finalized_at_date BETWEEN %s AND %s
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

    # Cálculo da média e desvio padrão para 1h, 12h e 24h
    df_1h = df.groupby(["merchant", "merchant_id", pd.Grouper(key="data_hora", freq="h")])[["volume", "quantidade"]].sum().reset_index()
    df_12h = df.groupby(["merchant", "merchant_id", pd.Grouper(key="data_hora", freq="12h")])[["volume", "quantidade"]].sum().reset_index()
    df_1d = df.groupby(["merchant", "merchant_id", pd.Grouper(key="data_hora", freq="d")])[["volume", "quantidade"]].sum().reset_index()

    metrics_1h = df_1h.groupby(["merchant_id", "merchant"]).agg({
        "volume": ["mean", "std"],
        "quantidade": ["mean", "std"]
    }).reset_index()
    metrics_1h.columns = ["merchant_id", "merchant", "mean_1h_volume", "std_1h_volume", "mean_1h_quantidade", "std_1h_quantidade"]

    metrics_12h = df_12h.groupby(["merchant_id", "merchant"]).agg({
        "volume": ["mean", "std"],
        "quantidade": ["mean", "std"]
    }).reset_index()
    metrics_12h.columns = ["merchant_id", "merchant", "mean_12h_volume", "std_12h_volume", "mean_12h_quantidade", "std_12h_quantidade"]

    metrics_1d = df_1d.groupby(["merchant_id", "merchant"]).agg({
        "volume": ["mean", "std"],
        "quantidade": ["mean", "std"]
    }).reset_index()
    metrics_1d.columns = ["merchant_id", "merchant", "mean_1d_volume", "std_1d_volume", "mean_1d_quantidade", "std_1d_quantidade"]

    result = metrics_1h.merge(metrics_12h, on=["merchant_id", "merchant"], how="outer").merge(metrics_1d, on=["merchant_id", "merchant"], how="outer")

    return result

############# SAQUES NA ÚLTIMA 1H, 12H, 24H #############
def get_recent_withdrawals(cursor):
    """
    Obtém os saques dos últimos 1h, 12h e 24h.
    """
    now = datetime.now(TZ_SP)
    last_1h = now - timedelta(hours=1)
    last_12h = now - timedelta(hours=12)
    last_24h = now - timedelta(hours=24)

    df_1h = get_withdrawals(cursor, last_1h, now).groupby(["merchant_id", "merchant"])["volume"].sum().reset_index()
    df_12h = get_withdrawals(cursor, last_12h, now).groupby(["merchant_id", "merchant"])["volume"].sum().reset_index()
    df_24h = get_withdrawals(cursor, last_24h, now).groupby(["merchant_id", "merchant"])["volume"].sum().reset_index()

    df_1h.columns = ["merchant_id", "merchant", "current_1h_withdrawals"]
    df_12h.columns = ["merchant_id", "merchant", "sum_12h_withdrawals"]
    df_24h.columns = ["merchant_id", "merchant", "sum_24h_withdrawals"]

    return df_1h.merge(df_12h, on=["merchant_id", "merchant"], how="outer").merge(df_24h, on=["merchant", "merchant_id"], how="outer") 

############# LOOP PRINCIPAL #############
def main():
    try:
        update_status("Atualizando...")

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                df_pix = count_pix_transactions(cursor)
                df_daily_pix = count_daily_transactions(cursor)
                df_revenue = daily_revenue(cursor)
                df_month_revenue = monthly_revenue(cursor)
                df_conversion = conversion_rate(cursor)
                df_fail = fail_rate(cursor)
                df_withdrawal_metrics = get_withdrawal_metrics(cursor)
                df_recent_withdrawals = get_recent_withdrawals(cursor)

                # Mescla os DataFrames corretamente usando `merchant_id`
                df_indicators = df_revenue.merge(df_pix, on=["merchant_id", "merchant"], how="left").fillna(0)
                df_indicators = df_indicators.merge(df_daily_pix, on=["merchant_id", "merchant"], how="left").fillna(0)
                df_indicators = df_indicators.merge(df_month_revenue, on=["merchant_id", "merchant"], how="outer", suffixes=('_daily', '_monthly'))
                df_indicators = df_indicators.merge(df_conversion, on=["merchant_id", "merchant"], how="outer", suffixes=('', '_conv'))
                df_indicators = df_indicators.merge(df_fail, on=["merchant_id", "merchant"], how="outer", suffixes=('', '_fail'))
                df_indicators = df_indicators.merge(df_withdrawal_metrics, on=["merchant_id", "merchant"], how="left")
                df_indicators = df_indicators.merge(df_recent_withdrawals,on=["merchant_id", "merchant"], how="left")
                
                # Envia para o Google Sheets
                wks_ind.set_dataframe(df_indicators, (2, 1), encoding="utf-8", copy_head=True)
                print("Indicadores atualizados.")

                # Atualiza o status com a data e hora da última atualização
                last_update = datetime.now(TZ_SP).strftime("%d/%m/%Y %H:%M:%S")
                update_status(f"Última atualização: {last_update}")

                # Reseta o trigger
                reset_trigger()

    except Exception as e:
        print(f"Erro inesperado: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise  # Re-lança a exceção para o workflow saber que houve erro

if __name__ == "__main__":
    main()