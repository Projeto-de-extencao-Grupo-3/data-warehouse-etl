import os
import pandas as pd


def limpar_feriado(input_path: str, output_path: str, headers: list[str], regex_dia: str) -> None:
    df = pd.read_csv(input_path, header=None)

    print(f"Dados ({os.path.basename(input_path)}):")
    print(df.head())

    df = df.dropna(axis=1, how="all")
    df.columns = headers

    df["DIA"] = (
        df["DIA"]
        .astype(str)
        .str.replace(regex_dia, "", regex=True, case=False)
        .str.strip()
    )

    df.to_csv(output_path, index=False)
    print(f"Arquivo salvo: {output_path}\n")

def limpar_datatran(input_path: str, output_path: str) -> None:

    df = pd.read_csv(
        input_path,
        sep=";",
        encoding="latin1",
        on_bad_lines="skip",
        engine="python",
    )
    print(f"Dados ({os.path.basename(input_path)}):")
    print(df.head())

    df = df.drop(columns=["br", "km", "latitude", "longitude", "delegacia", "uop", "uso_solo", "feridos_leves", "feridos_graves", "ignorados"], errors="ignore") 

    # Padronizando nomeclatura das colunas
    df.columns = df.columns.str.strip().str.upper()
    df = df.rename(columns={
        "DATA_INVERSA": "DATA",
        "UF": "ESTADO",
    })

    # Converter para datetime (NÃO converter para string depois)
    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")

    # Agora extrai mês direto
    df["MES"] = df["DATA"].dt.month

    df.to_csv(output_path, header=True, index=False)

def main() -> None:

    # Nacional
    limpar_feriado(
        input_path="./raw/raw_feriado_nacional.csv",
        output_path="./trusted/trusted_feriado_nacional.csv",
        headers=["DATA", "DIA", "TIPO", "DESCRICAO"],
        regex_dia=r"^\s*Dia d[eo]\s+",
    )

    # Municipal
    limpar_feriado(
        input_path="./raw/raw_feriado_municipal.csv",
        output_path="./trusted/trusted_feriado_municipal.csv",
        headers=["DATA", "DIA", "TIPO", "DESCRICAO", "ESTADO", "CODIGO_MUNICIPIO"],
        regex_dia=r"^\s*Dia d[eoa]\s+",
    )

    # Facultativo
    limpar_feriado(
        input_path="./raw/raw_feriado_facultativo.csv",
        output_path="./trusted/trusted_feriado_facultativo.csv",
        headers=["DATA", "DIA", "TIPO", "DESCRICAO", "ESTADO", "CODIGO_MUNICIPIO"],
        regex_dia=r"^\s*Dia d[eoa]\s+",
    )

    # Estadual
    limpar_feriado(
        input_path="./raw/raw_feriado_estadual.csv",
        output_path="./trusted/trusted_feriado_estadual.csv",
        headers=["DATA", "DIA", "TIPO", "DESCRICAO", "ESTADO"],
        regex_dia=r"^\s*Dia d[eoa]\s+",
    )

    # Juntar CSVs trusted
    df_estadual = pd.read_csv("./trusted/trusted_feriado_estadual.csv")
    df_facultativo = pd.read_csv("./trusted/trusted_feriado_facultativo.csv")
    df_nacional = pd.read_csv("./trusted/trusted_feriado_nacional.csv")
    df_municipal = pd.read_csv("./trusted/trusted_feriado_municipal.csv")

    df_final = pd.concat(
        [df_estadual, df_facultativo, df_nacional, df_municipal],
        ignore_index=True,
    )

    df_final.to_csv("./trusted/trusted_feriados.csv", index=False)
    print("Arquivo final salvo: ./trusted/trusted_feriados.csv")

    # Datatran
    limpar_datatran(
        input_path="./raw/raw_datatran.csv",
        output_path="./trusted/trusted_datatran1.csv",
    )


if __name__ == "__main__":
    main()