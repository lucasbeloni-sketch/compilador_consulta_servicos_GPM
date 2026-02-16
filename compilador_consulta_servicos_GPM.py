import os
import io
import base64
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

# =========================
# CONFIG
# =========================

PASTA_GOOGLE_DRIVE = "Consulta_Servico"

ARQUIVO_IGNORADO = "HISTÓRICO - EM001 e EM002 - 10.2023 até 03.2024.csv"

KEEP_COL_POS_1BASED = [1, 2, 3, 4, 5, 6, 7]  # colunas A:G

OUTPUT_CSV = "BANCO.csv"

# =========================
# GOOGLE DRIVE AUTH (CI/CD)
# =========================

def get_drive_service():
    b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")

    if not b64:
        raise Exception("Variável de ambiente GOOGLE_CREDENTIALS_B64 não definida")

    json_bytes = base64.b64decode(b64)
    creds_dict = json.loads(json_bytes.decode("utf-8"))

    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    return build("drive", "v3", credentials=creds)

# =========================
# DRIVE FUNCTIONS
# =========================

def get_folder_id(service, folder_name):
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = service.files().list(q=query, fields="files(id,name)").execute()
    files = res.get("files", [])
    if not files:
        raise Exception(f"Pasta não encontrada: {folder_name}")
    return files[0]["id"]

def list_csv_files(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    res = service.files().list(
        q=query,
        fields="files(id,name,mimeType)"
    ).execute()

    files = res.get("files", [])
    csvs = [f for f in files if f["name"].lower().endswith(".csv")]
    return csvs

def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = request.execute()
    fh.write(downloader)
    fh.seek(0)
    return fh.read()

# =========================
# DATA FUNCTIONS
# =========================

def keep_only_columns_by_position(df, positions_1based):
    idx = [p - 1 for p in positions_1based]
    return df.iloc[:, idx]

# =========================
# MAIN
# =========================

def main():
    print("[OK] Conectando ao Google Drive...")
    drive_service = get_drive_service()

    print(f"[OK] Buscando pasta: {PASTA_GOOGLE_DRIVE}")
    folder_id = get_folder_id(drive_service, PASTA_GOOGLE_DRIVE)

    print("[OK] Listando arquivos CSV...")
    csv_files = list_csv_files(drive_service, folder_id)
    print(f"[INFO] CSVs encontrados: {len(csv_files)}")

    bancos = []

    for f in csv_files:
        nome = f["name"]

        # =========================
        # IGNORA ARQUIVO PROBLEMÁTICO
        # =========================
        if nome.strip() == ARQUIVO_IGNORADO:
            print(f"[SKIP] Arquivo ignorado por regra: {nome}")
            continue

        print(f"[LOAD] {nome}")

        try:
            content = download_file(drive_service, f["id"])
            df = pd.read_csv(io.BytesIO(content), sep=";", dtype=str, low_memory=False)

            linhas = len(df)

            # adiciona coluna de origem
            df["arquivo_origem"] = nome

            # métricas
            preenchidos = df["dta_exec_srv"].notna().sum() if "dta_exec_srv" in df.columns else 0
            vazios = linhas - preenchidos
            perc = (preenchidos / linhas * 100) if linhas else 0

            print(f"[LOG] {nome} | linhas: {linhas} | dta_exec_srv preenchidos: {preenchidos} | em branco/nulos: {vazios} | preenchimento: {perc:.2f}%")

            bancos.append(df)

        except Exception as e:
            print(f"[ERRO] Falha ao ler {nome}: {e}")

    if not bancos:
        raise Exception("Nenhum CSV válido foi processado.")

    print("[OK] Concatenando arquivos...")
    banco_df = pd.concat(bancos, ignore_index=True)

    # =========================
    # PRESERVA COLUNA ORIGEM
    # =========================
    origem_col = banco_df["arquivo_origem"].copy()

    # =========================
    # MANTÉM COLUNAS PRINCIPAIS
    # =========================
    banco_df = keep_only_columns_by_position(banco_df, KEEP_COL_POS_1BASED)

    banco_df.columns = [
        "centro_servico",
        "Nota",
        "cod_pep_obra",
        "equipe",
        "obs_servico",
        "dta_exec_srv",
        "total_servicos"
    ]

    # =========================
    # REAPLICA COLUNA H
    # =========================
    banco_df["arquivo_origem"] = origem_col.values

    # =========================
    # EXPORTA
    # =========================
    banco_df.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    # =========================
    # LOG FINAL
    # =========================
    total = len(banco_df)
    preenchidos = banco_df["dta_exec_srv"].notna().sum()
    vazios = total - preenchidos
    perc = (preenchidos / total * 100) if total else 0

    print(f"[GLOBAL] BANCO TOTAL | linhas: {total} | dta_exec_srv preenchidos: {preenchidos} | em branco/nulos: {vazios} | preenchimento: {perc:.2f}%")
    print(f"[OK] Arquivo gerado: {OUTPUT_CSV}")

# =========================
# EXEC
# =========================

if __name__ == "__main__":
    main()
