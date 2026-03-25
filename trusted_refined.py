import pandas as pd
import os

def refine_data(trusted_path, refined_path):

    if not os.path.exists(refined_path):
        os.makedirs(refined_path)

    trusted_file = os.path.join(trusted_path, "feriados_tratado.csv")

    refined_file = os.path.join(refined_path, "feriados_refined.csv")
    estado_file = os.path.join(refined_path, "feriados_por_estado.csv")
    mes_file = os.path.join(refined_path, "feriados_por_mes.csv")
    categoria_file = os.path.join(refined_path, "feriados_por_categoria.csv")

    df = pd.read_csv(trusted_file)

    df["Data"] = pd.to_datetime(df["Data"])

    df["Ano"] = df["Data"].dt.year
    df["Mes"] = df["Data"].dt.month
    df["Dia"] = df["Data"].dt.day
    df["Nome_Mes"] = df["Data"].dt.month_name()

    df["Feriado_Nacional"] = df["Categoria"].apply(
        lambda x: "Sim" if x == "NACIONAL" else "Não"
    )

    df.to_csv(refined_file, index=False)

    feriados_estado = df.groupby("Estado").size().reset_index(name="Quantidade_Feriados")
    feriados_estado.to_csv(estado_file, index=False)

    feriados_mes = df.groupby("Mes").size().reset_index(name="Quantidade_Feriados")
    feriados_mes.to_csv(mes_file, index=False)

    feriados_categoria = df.groupby("Categoria").size().reset_index(name="Quantidade_Feriados")
    feriados_categoria.to_csv(categoria_file, index=False)

    print("Arquivos refined gerados")


if __name__ == "__main__":

    base_path = os.path.dirname(__file__)

    trusted_directory = os.path.join(base_path, "trusted")
    refined_directory = os.path.join(base_path, "refined")

    refine_data(trusted_directory, refined_directory)