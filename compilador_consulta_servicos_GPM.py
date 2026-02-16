import io
import os
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# =========================
# CONFIGURAÇÕES
# =========================

SERVICE_ACCOUNT_FILE = "service_account.json"  # credencial
FOLDER_ID = "SEU_FOLDER_ID_AQUI"                # ID da pasta no Drive

BANCO_OUTPUT = "BANCO.csv"

READ_CSV_KWARGS = {
    "sep": ";",
    "dtype": str,
    "encoding": "utf-8",
    "low_memory": False
}

# Colunas que serão mantidas (posição 1-based)
KEEP_COL_POS_1BASED = [1, 2, 3, 4, 5, 6, 7]

# Arquivo que será IGNORADO
SKIP_FILENAME = "HISTÓRICO - EM001 e EM002 - 10.2023 até 03.2024.csv"

# =========================
# AUTENTICAÇÃO
# =========================

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

# =========================
# DRIVE
# =========================

def list_csv_files(service, folder_id):
    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1000
    ).execute()
    return results.get("files", [])

def download_file(service, file_id, filename):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(filename, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

# =========================
# UTILS
# =========================

def keep_only_columns_by_position(df, positions_1based):
    idx = [p - 1 for p in positions_1based]
    return df.iloc[:, idx]

# =========================
# MAIN
# =========================

def main():
    drive_service = get_drive_service()
    csv_files = list_csv_files(drive_service, FOLDER_ID)

    print(f"[INFO] Arquivos encontrados: {len(csv_files)}")

    dfs = []
    temp_files = []

    for f in csv_files:
        name = f["name"].replace("/", "_")

        # 🚫 IGNORA ARQUIVO PROBLEMÁTICO
        if name == SKIP_FILENAME:
            print(f"[SKIP] Arquivo ignorado: {name}")
            continue

        print(f"[READ] Lendo arquivo: {name}")

        download_file(drive_service, f["id"], name)
        temp_files.append(name)

        try:
            df = pd.read_csv(name, **READ_CSV_KWARGS)

            # adiciona coluna de origem
            df["arquivo_origem"] = name

            dfs.append(df)

        except Exception as e:
            print(f"[ERRO] {name}: {e}")

    if not dfs:
        print("[ERRO] Nenhum CSV válido foi carregado.")
        return

    print("[INFO] Concatenando arquivos...")
    banco_df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    # =========================
    # PRESERVA COLUNA ORIGEM
    # =========================
    origem_col = banco_df["arquivo_origem"].copy()

    # mantém apenas colunas principais
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

    # reaplica coluna H
    banco_df["arquivo_origem"] = origem_col.values

    # =========================
    # EXPORTA
    # =========================
    banco_df.to_csv(BANCO_OUTPUT, index=False, sep=";", encoding="utf-8")

    print(f"[OK] BANCO gerado com sucesso: {BANCO_OUTPUT}")
    print(f"[INFO] Total de linhas: {len(banco_df)}")
    print(f"[INFO] Total de colunas: {len(banco_df.columns)}")

    # =========================
    # LIMPEZA TEMP
    # =========================
    for f in temp_files:
        try:
            os.remove(f)
        except:
            pass

    print("[CLEAN] Arquivos temporários removidos.")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    main()
