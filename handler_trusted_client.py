import io
import os
from urllib.parse import unquote_plus

import boto3
import pandas as pd

s3 = boto3.client("s3")


def _read_csv(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def _write_csv(bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")


def _refine_datatran(df: pd.DataFrame) -> pd.DataFrame:
    # client: SP + falhas mecânicas/elétricas
    df_sp = df[df["ESTADO"].astype(str).str.upper().str.strip().eq("SP")].copy()
    df_out = df_sp[df_sp["CAUSA_ACIDENTE"].astype(str).str.contains("falhas mec", case=False, na=False)].copy()
    return df_out


def _refine_feriados(df: pd.DataFrame) -> pd.DataFrame:
    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce", dayfirst=True)
    df["MES"] = df["DATA"].dt.month
    # client: somente SP
    if "ESTADO" in df.columns:
        df = df[df["ESTADO"].astype(str).str.upper().str.strip().eq("SP")].copy()
    return df


def lambda_handler(event, context):
    client_prefix = os.environ.get("CLIENT_PREFIX", "client/")
    processed = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        if not key.startswith("trusted/") or not key.lower().endswith(".csv"):
            continue

        df = _read_csv(bucket, key)

        lower_key = key.lower()
        if "datatran" in lower_key:
            df_out = _refine_datatran(df)
            out_name = "client_datatran_sp_falhas_mecanicas.csv"
        elif "feriado" in lower_key or "feriados" in lower_key:
            df_out = _refine_feriados(df)
            out_name = "client_feriados_sp.csv"
        else:
            continue

        out_key = f"{client_prefix}{out_name}"
        _write_csv(bucket, out_key, df_out)

        processed.append({"in": key, "out": out_key, "rows": len(df_out)})

    return {"statusCode": 200, "processed": processed}