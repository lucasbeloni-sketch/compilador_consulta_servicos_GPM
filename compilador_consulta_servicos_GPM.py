import os
import json
import base64
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# =========================
# CONFIG
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

NEW_FOLDER_ID = "1QHtqMNCcIzNihwnu3copkNmBZnaL6Z6z"
OUTPUT_CSV_NAME = "BANCO.csv"
FOLDER_ID = "17IobcQeVLs83rUCqWKTi18yXiAPbupjf"
SPREADSHEET_ID = "1B_ZAktVrIoY_qGg9vhjMabmNqGMeHODtWPR8nmFp61A"
SHEET_NAME = "BD_ConsultaServ"

UPLOAD_BANCO_PARA_DRIVE = True

READ_CSV_KWARGS = dict(
    dtype=str,
    encoding="utf-8-sig",
    sep=None,
    engine="python"
)

KEEP_COL_POS_1BASED = [47, 6, 27, 50, 52, 68, 70]

# =========================
# AUTH
# =========================
def get_credentials():
    secret = os.getenv("GOOGLE_CREDENTIALS_B64")
    if not secret:
        raise ValueError("O secret 'GOOGLE_CREDENTIALS_B64' não foi encontrado ou está vazio!")

    credentials_json = base64.b64decode(secret).decode("utf-8")
    info = json.loads(credentials_json)

    return service_account.Credentials.from_service_account_info(
        info, scopes=SCOPES
    )

def get_drive_service():
    return build("drive", "v3", credentials=get_credentials())

def get_sheets_service():
    return build("sheets", "v4", credentials=get_credentials())

# =========================
# DRIVE HELPERS
# =========================
def list_files(service, folder_id, drive_id):
    query = f"'{folder_id}' in parents and trashed = false"
    files = []
    token = None

    while True:
        resp = service.files().list(
            q=query,
            pageToken=token,
            pageSize=1000,
            fields="nextPageToken, files(id,name,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="drive",
            driveId=drive_id,
        ).execute()

        files.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break

    return files

def download_file(service, file_id, filename):
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(filename, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

def find_file_in_folder(service, folder_id, drive_id, filename):
    query = f"'{folder_id}' in parents and trashed = false and name = '{filename}'"
    resp = service.files().list(
        q=query,
        fields="files(id,name)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="drive",
        driveId=drive_id,
    ).execute()

    files = resp.get("files", [])
    return files[0]["id"] if files else None

def upload_or_update_banco(drive_service, folder_id, drive_id, local_path, filename):
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    existing_id = find_file_in_folder(drive_service, folder_id, drive_id, filename)

    if existing_id:
        drive_service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        return "updated"

    drive_service.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=media,
        supportsAllDrives=True
    ).execute()
    return "created"

# =========================
# DATA HELPERS
# =========================
def keep_only_columns_by_position(df, positions_1based):
    idx = [p - 1 for p in positions_1based]
    return df.iloc[:, idx]

def to_number_ptbr(value):
    if value is None:
        return 0.0
    s = str(value).strip()
    if s == "" or s.lower() in ["nan", "none"]:
        return 0.0
    s = s.replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

# =========================
# SHEETS HELPERS
# =========================
def clear_range(service, spreadsheet_id, range_):
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=range_
    ).execute()

def upload_to_sheets(service, df):
    df_sheets = df.iloc[:, :7].copy()
    df_sheets = df_sheets.fillna("")
    values = df_sheets.values.tolist()

    clear_range(service, SPREADSHEET_ID, f"{SHEET_NAME}!A3:G")

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A3",
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()

    timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!B1",
        valueInputOption="USER_ENTERED",
        body={"values": [[timestamp]]}
    ).execute()

# =========================
# MAIN
# =========================
def main():
    drive_service = get_drive_service()
    sheets_service = get_sheets_service()

    folder = drive_service.files().get(
        fileId=NEW_FOLDER_ID,
        fields="id,name,driveId",
        supportsAllDrives=True
    ).execute()

    drive_id = folder["driveId"]
    print(f"[OK] Pasta: {folder['name']}")

    files = list_files(drive_service, NEW_FOLDER_ID, drive_id)

    csv_files = [
        f for f in files
        if f["name"].lower().endswith(".csv")
        and f["name"] != OUTPUT_CSV_NAME
    ]

    print(f"[INFO] CSVs encontrados: {len(csv_files)}")

    dfs = []
    temp_files = []

    for f in csv_files:
        name = f["name"].replace("/", "_")
        download_file(drive_service, f["id"], name)
        temp_files.append(name)

        try:
            df = pd.read_csv(name, **READ_CSV_KWARGS)
            df["arquivo_origem"] = name
            dfs.append(df)
        except Exception as e:
            print(f"[ERRO] {name}: {e}")

    for f in temp_files:
        try:
            os.remove(f)
        except:
            pass

    if not dfs:
        print("[ERRO] Nenhum CSV válido.")
        return

    banco_df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    origem_col = banco_df["arquivo_origem"].copy()

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

    banco_df["arquivo_origem"] = origem_col.values

    banco_df["cod_pep_obra"] = banco_df["cod_pep_obra"].fillna("").astype(str).str.upper()

    # =========================
    # TRATAMENTO ROBUSTO DE DATA
    # =========================
    banco_df["dta_exec_srv"] = (
        banco_df["dta_exec_srv"]
        .astype(str)
        .str.strip()
        .str.replace("\u200b", "", regex=False)
        .str.replace("\xa0", " ", regex=False)
        .replace({"": None, "nan": None, "NaT": None, "None": None})
    )

    banco_df["dta_exec_srv"] = pd.to_datetime(
        banco_df["dta_exec_srv"],
        errors="coerce",
        dayfirst=True
    )

    total = len(banco_df)
    validas = banco_df["dta_exec_srv"].notna().sum()
    invalidas = total - validas
    print(f"[DATA] Total: {total} | Válidas: {validas} | Inválidas: {invalidas}")

    banco_df["total_servicos"] = banco_df["total_servicos"].apply(to_number_ptbr)

    banco_df = banco_df.sort_values(
        by="dta_exec_srv",
        ascending=True,
        kind="mergesort"
    ).reset_index(drop=True)

    banco_df["dta_exec_srv"] = banco_df["dta_exec_srv"].dt.strftime("%d/%m/%Y")

    banco_df.to_csv(
        OUTPUT_CSV_NAME,
        index=False,
        encoding="utf-8-sig",
        sep=";",
        decimal=",",
        float_format="%.2f"
    )

    upload_to_sheets(sheets_service, banco_df)

    if UPLOAD_BANCO_PARA_DRIVE:
        action = upload_or_update_banco(
            drive_service,
            folder_id=FOLDER_ID,
            drive_id=drive_id,
            local_path=OUTPUT_CSV_NAME,
            filename=OUTPUT_CSV_NAME
        )
        print(f"[OK] BANCO.csv enviado ao Drive ({action}).")

    print("[OK] Processo finalizado com sucesso.")

if __name__ == "__main__":
    main()
