import pandas as pd
import os

def clean_data(raw_path, trusted_path):

    if not os.path.exists(trusted_path):
        os.makedirs(trusted_path)

    raw_file = os.path.join(raw_path, "feriados_completo.csv")
    trusted_file = os.path.join(trusted_path, "feriados_tratado.csv")

    df = pd.read_csv(raw_file)

    # remove possível cabeçalho duplicado
    df = df[df["Data"] != "Data"]

    # converte a data
    df["Data"] = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df = df.dropna(subset=["Data"])

    # remove linhas com dados importantes vazios
    df = df.dropna(subset=["Nome do Feriado", "Categoria", "Estado"])

    # preenche descrição vazia
    df["Descrição"] = df["Descrição"].fillna("Sem descrição")

    # limpeza básica de texto
    df["Nome do Feriado"] = df["Nome do Feriado"].astype(str).str.strip()
    df["Categoria"] = df["Categoria"].astype(str).str.strip()
    df["Descrição"] = df["Descrição"].astype(str).str.strip()
    df["Estado"] = df["Estado"].astype(str).str.strip().str.upper()

    # remove duplicados
    df = df.drop_duplicates()

    # ordena pela data
    df = df.sort_values(by="Data")

    # salva resultado
    df.to_csv(trusted_file, index=False)

    print("Arquivo trusted gerado:", trusted_file)


if __name__ == "__main__":

    base_path = os.path.dirname(__file__)

    raw_directory = os.path.join(base_path, "raw")
    trusted_directory = os.path.join(base_path, "trusted")

    clean_data(raw_directory, trusted_directory)