import gspread
import pandas as pd
from flask import Flask, jsonify
from google.oauth2.service_account import Credentials

# ================== CẤU HÌNH ==================
SERVICE_ACCOUNT_FILE = "service_account.json"
SHEET_ID = "1XiGxHxrygIQ3fiwnA6mgYH649ituRUQJaHy0L0ON1eo"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

app = Flask(__name__)

# ================== HÀM CHÍNH ==================
def run_ranking():
    # 1. Kết nối Google Sheets
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)

    sheet = spreadsheet.worksheet("Data")
    df = pd.DataFrame(sheet.get_all_records())

    # 2. Làm sạch dữ liệu
    cols_to_clean = [
        "Số buổi điểm danh",
        "Điểm quá trình",
        "Điểm giữa kỳ",
        "Điểm cuối kỳ"
    ]

    for col in cols_to_clean:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .replace("", "0")
            .astype(float)
        )

    # 3. Tính điểm tổng
    df["Điểm tổng"] = (
        0.1 * df["Số buổi điểm danh"] +
        0.3 * df["Điểm quá trình"] +
        0.2 * df["Điểm giữa kỳ"] +
        0.4 * df["Điểm cuối kỳ"]
    ).round(2)

    # 4. Xếp hạng cá nhân
    df["Hạng cá nhân"] = (
        df["Điểm tổng"]
        .rank(ascending=False, method="min")
        .astype(int)
    )

    df = df.sort_values("Điểm tổng", ascending=False).reset_index(drop=True)

    df_individual = df[
        ["Mã nhóm", "MSSV", "Họ tên", "Điểm tổng", "Hạng cá nhân"]
    ]

    # 5. Xếp hạng nhóm
    df_group = (
        df.groupby("Mã nhóm")
        .agg(
            Điểm_trung_bình_nhóm=("Điểm tổng", "mean"),
            Thành_viên=("Họ tên", lambda x: ", ".join(x)),
            Số_thành_viên=("Họ tên", "count")
        )
        .reset_index()
    )

    df_group["Điểm_trung_bình_nhóm"] = (
        df_group["Điểm_trung_bình_nhóm"].round(2)
    )

    df_group["Hạng nhóm"] = (
        df_group["Điểm_trung_bình_nhóm"]
        .rank(ascending=False, method="min")
        .astype(int)
    )

    df_group = df_group.sort_values(
        "Điểm_trung_bình_nhóm", ascending=False
    ).reset_index(drop=True)

    # 6. Ghi Google Sheets
    def update_sheet(name, dataframe):
        try:
            ws = spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(
                title=name,
                rows=str(len(dataframe) + 10),
                cols=str(len(dataframe.columns) + 5)
            )

        ws.clear()
        ws.update(
            [dataframe.columns.tolist()] +
            dataframe.values.tolist()
        )

    update_sheet("Ranking_Individual", df_individual)
    update_sheet("Ranking_Group", df_group)

# ================== API ==================
@app.route("/run-ranking", methods=["GET"])
def run_api():
    run_ranking()
    return jsonify({
        "status": "success",
        "message": "Ranking updated successfully"
    })

# ================== MAIN ==================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
