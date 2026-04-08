import pandas as pd
import os

def refinar_feriados() -> None:
    df = pd.read_csv("./trusted/trusted_feriados.csv")

    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce", dayfirst=True)
    df["ANO"] = df["DATA"].dt.year
    df["MES"] = df["DATA"].dt.month
    df["DIA"] = df["DATA"].dt.day
    df["NOME_MES"] = df["DATA"].dt.month_name()

    df["FERIADO_NACIONAL"] = df["TIPO"].astype(str).str.upper().eq("NACIONAL").map({True: "Sim", False: "Não"})

    df.to_csv("./refined/refined_feriados_nacional.csv", index=False)
    df.to_csv("./refined/refined_feriados_mes.csv", index=False)  

    df[df["ESTADO"].astype(str).str.upper().str.strip().eq("SP")] \
        .to_csv("./refined/refined_feriados_sp.csv", index=False)

    df.groupby("ESTADO", dropna=False).size().reset_index(name="Quantidade_Feriados") \
        .to_csv("./refined/refined_feriados_por_estado.csv", index=False)

    df.groupby("MES", dropna=False).size().reset_index(name="Quantidade_Feriados") \
        .to_csv("./refined/refined_feriados_por_mes.csv", index=False)

    df.groupby("TIPO", dropna=False).size().reset_index(name="Quantidade_Feriados") \
        .to_csv("./refined/refined_feriados_por_categoria.csv", index=False)

    print("Refinamento FERIADOS concluído.")



def refinar_datatran() -> None:
    df = pd.read_csv("./trusted/trusted_datatran.csv")

    # SP
    df_sp = df[df["ESTADO"].astype(str).str.upper().str.strip() == "SP"].copy()
    df_sp.to_csv("./refined/refined_datatran_sp.csv", index=False)

    # SP + falhas mecânicas/elétricas
    df_sp_falhas = df_sp[
        df_sp["CAUSA_ACIDENTE"].astype(str).str.contains("falhas mec", case=False, na=False)
    ].copy()
    df_sp_falhas.to_csv("./refined/refined_datatran_sp_falhas_mecanicas.csv", index=False)

    print("Refinamento DATATRAN concluído.")

def main() -> None:
    refinar_feriados()
    refinar_datatran()
    print("Refinamento finalizado.")

if __name__ == "__main__":
    main()