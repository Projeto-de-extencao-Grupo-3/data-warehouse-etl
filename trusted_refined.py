import pandas as pd
import os

def refine_data():
    df = pd.read_csv("./trusted/trusted_feriados.csv")

    df["DATA"] = pd.to_datetime(df["DATA"])

    df["ANO"] = df["DATA"].dt.year
    df["MES"] = df["DATA"].dt.month
    df["DIA"] = df["DATA"].dt.day
    df["NOME_MES"] = df["DATA"].dt.month_name()

    df["FERIADO_NACIONAL"] = df["TIPO"].apply(
        lambda x: "Sim" if x == "NACIONAL" else "Não"
    )

    df.to_csv("./refined/refined_feriados_nacional.csv", index=False)

    feriados_estado = df.groupby("ESTADO").size().reset_index(name="Quantidade_Feriados")
    feriados_estado.to_csv("./refined/refined_feriados_por_estado.csv", index=False)

    feriados_mes = df.groupby("MES").size().reset_index(name="Quantidade_Feriados")
    feriados_mes.to_csv("./refined/refined_feriados_por_mes.csv", index=False)

    feriados_categoria = df.groupby("TIPO").size().reset_index(name="Quantidade_Feriados")
    feriados_categoria.to_csv("./refined/refined_feriados_por_categoria.csv", index=False)

    print("Arquivos refined gerados")


if __name__ == "__main__":
    refine_data()