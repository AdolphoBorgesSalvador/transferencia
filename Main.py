import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def conectar_postgres():
    """Conecta ao PostgreSQL no Docker"""
    try:
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME", "postgres")
        username = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "")

        connection_string = (
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        engine = create_engine(connection_string)
        print("✅ Conexão com PostgreSQL estabelecida!")
        return engine

    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return None


def get_queries():
    """Retorna as queries SQL necessárias para a análise"""
    query_zmb51 = """
    -- zmb51: movimentações no último ano
    SELECT
        material,
        centro,
        qtd_um_registro,
        canal,
        data_de_lancamento
    FROM power_bi.zmb51
    WHERE material IN (
        'A8K3430',
        'A8K3230',
        'AAV8230',
        'AAV8330',
        'T671600'
    )
    AND data_de_lancamento >= CURRENT_DATE - INTERVAL '12 months';
    """

    query_zstok = """
    -- zstok: estoque atual por material
    SELECT
        material,
        estoque_total,
        cen AS centro
    FROM power_bi.zstok
    WHERE material IN (
        'A8K3430',
        'A8K3230',
        'AAV8230',
        'AAV8330',
        'T671600'
    );
    """

    query_fup = """
    -- fup: previsões de entrada por material
    SELECT
        material,
        qtde_pedido,
        data_prev_entrada,
        data_de_remessa
    FROM power_bi.fup
    WHERE material IN (
        'A8K3430',
        'A8K3230',
        'AAV8230',
        'AAV8330',
        'T671600'
    );
    """
    return query_zmb51, query_zstok, query_fup


def load_data(engine):
    """Carrega os dados do banco de dados"""
    query_zmb51, query_zstok, query_fup = get_queries()

    zmb51 = pd.read_sql_query(query_zmb51, engine)
    zstok = pd.read_sql_query(query_zstok, engine)
    fup = pd.read_sql_query(query_fup, engine)

    return zmb51, zstok, fup


def process_zstok(zstok):
    """Processa os dados do zstok"""
    return zstok.pivot_table(index="material", columns="centro", values="estoque_total")


def process_zmb51(zmb51):
    """Processa os dados do zmb51"""
    zmb51["data_de_lancamento"] = pd.to_datetime(zmb51["data_de_lancamento"])
    zmb51["ano_mes"] = zmb51["data_de_lancamento"].dt.to_period("M")

    # Separar dados CE07 e não CE07
    zmb51_sem_ce07 = zmb51[zmb51["centro"] != "CE07"]
    zmb51_ce07 = zmb51[zmb51["centro"] == "CE07"]

    # Criar pivots
    zmb51_pivot_sem_ce07 = create_pivot(zmb51_sem_ce07)
    zmb51_pivot_ce07 = create_pivot(zmb51_ce07)

    return zmb51_pivot_sem_ce07, zmb51_pivot_ce07


def create_pivot(df):
    """Cria tabela pivot para análise temporal"""
    pivot = df.pivot_table(
        index="material", columns="ano_mes", values="qtd_um_registro", aggfunc="sum"
    )
    pivot = pivot.sort_index(axis=1)

    # Adicionar médias
    pivot["media_3m"] = pivot.iloc[:, -3:].mean(axis=1)
    pivot["media_6m"] = pivot.iloc[:, -6:].mean(axis=1)

    return pivot


def create_final_datasets(zmb51_pivot_ce07, zmb51_pivot_sem_ce07, zstok_pivot):
    """Cria os datasets finais para CE07 e demais centros"""
    # Dataset CE07
    zstok_pivot_ce07 = zstok_pivot[["CE07"]]
    zmb51_estoque_ce07 = pd.concat([zmb51_pivot_ce07, zstok_pivot_ce07], axis=1)

    # Dataset sem CE07
    zstok_pivot_sem_ce07 = zstok_pivot.drop(columns=["CE07"])

    # Dados de redução
    dados_reducao = pd.DataFrame(
        {
            "material": ["A8K3430", "A8K3230", "AAV8230", "AAV8330", "T671600"],
            "possivel_reducao": [11, 4, 50, 30, 2],
        }
    ).set_index("material")

    zmb51_estoque_sem_ce07 = pd.concat(
        [zmb51_pivot_sem_ce07, zstok_pivot_sem_ce07, dados_reducao], axis=1
    )

    return zmb51_estoque_ce07, zmb51_estoque_sem_ce07


def export_to_json(df, filename):
    """Exporta DataFrame para arquivo JSON"""
    output_path = Path("output")
    output_path.mkdir(parents=True, exist_ok=True)

    filepath = output_path / filename

    df.reset_index().to_json(filepath, orient="records", indent=4)
    print(f"✅ Arquivo exportado: {filepath}")


def main():
    # Conectar ao banco de dados
    engine = conectar_postgres()
    if not engine:
        return

    # Carregar dados
    zmb51, zstok, fup = load_data(engine)

    # Processar dados
    zstok_pivot = process_zstok(zstok)
    zmb51_pivot_sem_ce07, zmb51_pivot_ce07 = process_zmb51(zmb51)

    # Criar datasets finais
    zmb51_estoque_ce07, zmb51_estoque_sem_ce07 = create_final_datasets(
        zmb51_pivot_ce07, zmb51_pivot_sem_ce07, zstok_pivot
    )

    # Exportar resultados
    export_to_json(zmb51_estoque_ce07, "zmb51_estoque_ce07.json")
    export_to_json(zmb51_estoque_sem_ce07, "zmb51_estoque_sem_ce07.json")


if __name__ == "__main__":
    main()
