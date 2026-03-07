from typing import Optional, Tuple
import re
import json
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import io

# ─── ストレージ設定（Supabase優先、なければローカルJSON）─────────────────────
DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
MASTER_PATH  = os.path.join(DATA_DIR, "master_data.json")
INVOICE_PATH = os.path.join(DATA_DIR, "invoice_data.json")
CONFIG_PATH  = os.path.join(DATA_DIR, "config.json")
os.makedirs(DATA_DIR, exist_ok=True)

DATETIME_COLS = ["利用日", "支払日"]

def _use_supabase() -> bool:
    try:
        url = st.secrets.get("supabase", {}).get("url", "")
        return url.startswith("https://") and "supabase.co" in url
    except Exception:
        return False

@st.cache_resource
def _get_supabase():
    from supabase import create_client
    return create_client(st.secrets["supabase"]["url"], st.secrets["supabase"]["key"])


# ─── DataFrame ⇔ レコード変換 ─────────────────────────────────────────────────
def _df_to_records(df: pd.DataFrame) -> list:
    d = df.copy()
    for col in DATETIME_COLS:
        if col in d.columns:
            d[col] = d[col].apply(lambda v: v.isoformat() if pd.notna(v) else None)
    return json.loads(d.to_json(orient="records", force_ascii=False))


def _records_to_df(records: list) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    for col in DATETIME_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "確認済み" in df.columns:
        df["確認済み"] = df["確認済み"].astype(bool)
    return df


def _df_to_json(df: pd.DataFrame) -> str:
    return json.dumps(_df_to_records(df), ensure_ascii=False)


def _json_to_df(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    with open(path, encoding="utf-8") as f:
        records = json.load(f)
    return _records_to_df(records)


# ─── 保存・読込（Supabase / ローカル自動切替）────────────────────────────────
def save_state():
    config = {
        "stores":         st.session_state.stores,
        "platform_fees":  st.session_state.platform_fees,
        "fixed_costs":    st.session_state.fixed_costs,
        "business_costs": st.session_state.business_costs,
        "store_mapping":  st.session_state.store_mapping,
    }
    if _use_supabase():
        sb = _get_supabase()
        sb.table("app_storage").upsert({"key": "master_data",  "value": _df_to_records(st.session_state.master_data)}).execute()
        sb.table("app_storage").upsert({"key": "invoice_data", "value": _df_to_records(st.session_state.invoice_data)}).execute()
        sb.table("app_storage").upsert({"key": "config",       "value": config}).execute()
    else:
        with open(MASTER_PATH,  "w", encoding="utf-8") as f:
            f.write(_df_to_json(st.session_state.master_data))
        with open(INVOICE_PATH, "w", encoding="utf-8") as f:
            f.write(_df_to_json(st.session_state.invoice_data))
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)


def load_state():
    if "state_loaded" in st.session_state:
        return
    if _use_supabase():
        sb = _get_supabase()
        rows = {r["key"]: r["value"] for r in sb.table("app_storage").select("key, value").execute().data}
        st.session_state.master_data  = _records_to_df(rows.get("master_data") or [])
        st.session_state.invoice_data = _records_to_df(rows.get("invoice_data") or [])
        cfg = rows.get("config") or {}
    else:
        st.session_state.master_data  = _json_to_df(MASTER_PATH)
        st.session_state.invoice_data = _json_to_df(INVOICE_PATH)
        cfg = {}
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)
    st.session_state.stores         = cfg.get("stores",         ["元町駅前店", "加古川駅前店", "加古川今福店"])
    st.session_state.platform_fees  = cfg.get("platform_fees",  {"よやクル": 10.0, "インスタベース": 30.0, "スペースマーケット": 30.0})
    st.session_state.fixed_costs    = cfg.get("fixed_costs",    {})
    st.session_state.business_costs = cfg.get("business_costs", {})
    st.session_state.store_mapping  = cfg.get("store_mapping",  {})
    st.session_state.state_loaded  = True

st.set_page_config(
    page_title="レンタルスタジオ 売上管理",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

PLATFORMS = ["よやクル", "インスタベース", "スペースマーケット"]

# ─── プラットフォーム自動検出スキーマ ──────────────────────────────────────────
PLATFORM_SCHEMAS = {
    "よやクル": {
        "signature":      ["決済元金", "決済ID", "割引金額"],
        "date":           "決済日時（データ入力用）",   # 支払日（年付き）
        "usage_date":     "利用日時",                  # 利用日（MM/DD形式）
        "store":          "スペース名",
        "amount":         "決済元金",
        "refund":         "返金額",
        "discount":       "割引金額",
        "booking_id":     "決済ID",
        "customer":       "HN",
        "net_amount":     None,
        "payment_method": "決済方法",
        "bank_transfer_keywords": ["オフライン決済"],   # 手動確認が必要な決済方法
    },
    "インスタベース": {
        "signature":      ["予約金額 (税込)", "支払金額 (税込)", "予約者名"],
        "date":           "利用開始日時",
        "usage_date":     "利用開始日時",
        "store":          "施設名",
        "amount":         "予約金額 (税込)",
        "refund":         None,
        "discount":       None,
        "booking_id":     "予約ID",
        "customer":       "予約者名",
        "net_amount":     "支払金額 (税込)",
        "payment_method": "決済方法",
        "bank_transfer_keywords": [],
    },
    "スペースマーケット": {
        "signature":      ["成約金額", "振込予定金額", "ゲスト名"],
        "date":           "実施日",
        "usage_date":     "実施日",
        "store":          "施設名",
        "amount":         "成約金額",
        "refund":         None,
        "discount":       None,
        "booking_id":     "予約ID",
        "customer":       "ゲスト名",
        "net_amount":     "振込予定金額",
        "payment_method": "お支払い方法",
        "bank_transfer_keywords": [],
    },
}

MONTH_OPTIONS = (
    pd.date_range(
        start=f"{datetime.now().year - 1}-01",
        periods=24, freq="ME",
    ).strftime("%Y-%m").tolist()
)


# ─── セッション初期化 ────────────────────────────────────────────────────────
def init_session():
    # まずファイルから復元を試みる
    load_state()
    # ファイルがなかった項目だけデフォルト値をセット
    defaults = {
        "stores":         ["元町駅前店", "加古川駅前店", "加古川今福店"],
        "platform_fees":  {"よやクル": 10.0, "インスタベース": 30.0, "スペースマーケット": 30.0},
        "fixed_costs":    {},
        "business_costs": {},   # 事業全体の経費（店舗に紐づかない）
        "master_data":    pd.DataFrame(),
        "invoice_data":   pd.DataFrame(),
        "store_mapping":  {},
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


init_session()


# ─── ヘルパー ────────────────────────────────────────────────────────────────
def parse_amount(series: pd.Series) -> pd.Series:
    """¥記号・カンマを除去して数値に変換（半角・全角どちらも対応）"""
    return pd.to_numeric(
        series.astype(str)
              .str.replace("¥", "", regex=False)   # 半角 ¥
              .str.replace("￥", "", regex=False)   # 全角 ￥
              .str.replace(",", "", regex=False)
              .str.strip(),
        errors="coerce",
    )


def parse_yoyakuru_usage_date(usage_series: pd.Series,
                               payment_series: pd.Series) -> pd.Series:
    """よやクルの利用日時 "MM/DD (曜) HH:MM〜HH:MM" を年付き日付に変換"""
    results = []
    for usage, payment in zip(usage_series, payment_series):
        try:
            m = re.match(r"(\d{1,2})/(\d{1,2})", str(usage))
            if m and pd.notna(payment):
                month, day = int(m.group(1)), int(m.group(2))
                year = payment.year
                # 支払い月より利用月が早い場合は翌年（例：12月支払→1月利用）
                if payment.month - month > 6:
                    year += 1
                results.append(pd.Timestamp(year=year, month=month, day=day))
            else:
                results.append(pd.NaT)
        except Exception:
            results.append(pd.NaT)
    return pd.Series(results, index=usage_series.index)


def apply_store_mapping(platform: str, name: str) -> str:
    key = f"{platform}_{name}"
    return st.session_state.store_mapping.get(key, name)


def fmt_yen(v):
    return f"¥{v:,.0f}"


def color_profit(v):
    if isinstance(v, (int, float)):
        return "color: #28a745" if v >= 0 else "color: #dc3545"
    return ""


def _rebuild_cache():
    """master_data / invoice_data が変わった時だけ呼ぶ。結合結果をキャッシュ。"""
    md  = st.session_state.master_data
    inv = st.session_state.invoice_data

    all_frames       = [f for f in [md, inv]                  if not f.empty]
    confirmed_frames = [f for f in [md[md["確認済み"] == True] if not md.empty else pd.DataFrame(),
                                    inv[inv["確認済み"] == True] if not inv.empty else pd.DataFrame()]
                        if not f.empty]

    st.session_state._all_data       = pd.concat(all_frames,       ignore_index=True) if all_frames       else pd.DataFrame()
    st.session_state._confirmed_data = pd.concat(confirmed_frames, ignore_index=True) if confirmed_frames else pd.DataFrame()
    # サイドバー用カウントも更新
    st.session_state._total_records    = len(st.session_state._all_data)
    st.session_state._pending_count    = int((~st.session_state._all_data["確認済み"]).sum()) if not st.session_state._all_data.empty and "確認済み" in st.session_state._all_data.columns else 0


def get_confirmed_data() -> Optional[pd.DataFrame]:
    """確認済みデータ（キャッシュから取得）"""
    if "_confirmed_data" not in st.session_state:
        _rebuild_cache()
    df = st.session_state._confirmed_data
    return df if not df.empty else None


def get_all_data() -> Optional[pd.DataFrame]:
    """全データ（キャッシュから取得）"""
    if "_all_data" not in st.session_state:
        _rebuild_cache()
    df = st.session_state._all_data
    return df if not df.empty else None


def detect_platform(cols: list) -> Optional[str]:
    for platform, schema in PLATFORM_SCHEMAS.items():
        if all(sig in cols for sig in schema["signature"]):
            return platform
    return None


def process_csv(df_raw: pd.DataFrame, platform: str) -> Tuple[int, int]:
    """CSVを正規化してmaster_dataにマージ（重複は予約IDでスキップ）"""
    schema = PLATFORM_SCHEMAS[platform]

    # ── 日付処理 ──
    payment_dates = pd.to_datetime(df_raw[schema["date"]], errors="coerce")
    if platform == "よやクル":
        usage_dates = parse_yoyakuru_usage_date(df_raw[schema["usage_date"]], payment_dates)
    else:
        usage_dates = pd.to_datetime(df_raw[schema["usage_date"]], errors="coerce")

    # ── 金額処理 ──
    売上 = parse_amount(df_raw[schema["amount"]])
    返金 = parse_amount(df_raw[schema["refund"]]).fillna(0).abs() if schema["refund"] else pd.Series(0, index=df_raw.index)
    割引 = parse_amount(df_raw[schema["discount"]]).fillna(0).abs() if schema["discount"] else pd.Series(0, index=df_raw.index)
    実売上 = 売上 - 返金 - 割引

    # ── 決済方法 ──
    決済方法 = (
        df_raw[schema["payment_method"]].astype(str)
        if schema.get("payment_method") and schema["payment_method"] in df_raw.columns
        else pd.Series("不明", index=df_raw.index)
    )

    # ── 銀行振込かどうか判定 ──
    keywords = schema.get("bank_transfer_keywords", [])
    is_bank = 決済方法.apply(lambda v: any(k in str(v) for k in keywords)) if keywords else pd.Series(False, index=df_raw.index)

    # ── 手数料計算 ──
    if schema["net_amount"] and schema["net_amount"] in df_raw.columns:
        手取り   = parse_amount(df_raw[schema["net_amount"]]).fillna(0)
        手数料   = (実売上 - 手取り).clip(lower=0)
        手数料率 = (手数料 / 実売上.replace(0, pd.NA) * 100).fillna(0).round(1)
    else:
        fee_rate = st.session_state.platform_fees[platform]
        手数料率 = pd.Series(fee_rate, index=df_raw.index)
        手数料   = 実売上 * fee_rate / 100
        手取り   = 実売上 - 手数料

    dp = pd.DataFrame({
        "利用日":         usage_dates,
        "支払日":         payment_dates,
        "月":             usage_dates.dt.strftime("%Y-%m"),
        "店舗":           df_raw[schema["store"]].astype(str).apply(lambda n: apply_store_mapping(platform, n)),
        "顧客名":         df_raw[schema["customer"]].astype(str) if schema["customer"] else "不明",
        "予約ID":         df_raw[schema["booking_id"]].astype(str) if schema["booking_id"] else "",
        "決済方法":       決済方法,
        "売上":           売上,
        "割引":           割引,
        "返金":           返金,
        "実売上":         実売上,
        "手数料率":       手数料率,
        "手数料":         手数料,
        "手取り":         手取り,
        "プラットフォーム": platform,
        "データ種別":     "CSV",
        "確認済み":       ~is_bank,   # 銀行振込のみ False
    })

    # 月が取れなかった行は支払日の月で補完
    mask = dp["月"].isna() | (dp["月"] == "NaT")
    dp.loc[mask, "月"] = dp.loc[mask, "支払日"].dt.strftime("%Y-%m")

    dp = dp.dropna(subset=["売上"])

    existing = st.session_state.master_data
    if existing.empty:
        merged = dp
    else:
        merged = pd.concat([existing, dp], ignore_index=True)
        merged = merged.drop_duplicates(subset=["プラットフォーム", "予約ID"], keep="first")

    new_count = len(merged) - len(existing)
    dup_count = len(dp) - new_count
    st.session_state.master_data = merged.reset_index(drop=True)
    return new_count, dup_count


# ─── サイドバー ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏢 合同会社ScalePro")
    page = st.radio(
        "ナビゲーション",
        ["📊 ダッシュボード", "📥 データ取込", "📄 請求書管理",
         "⚙️ 設定", "📈 損益レポート", "👥 顧客分析", "🔍 予約検索"],
        label_visibility="collapsed",
    )
    st.divider()
    if "_total_records" not in st.session_state:
        _rebuild_cache()
    total_records = st.session_state._total_records
    pending       = st.session_state._pending_count
    st.caption(f"登録店舗: {len(st.session_state.stores)} 店舗")
    st.caption(f"総レコード: {total_records:,} 件")
    if pending > 0:
        st.warning(f"入金確認待ち: {pending} 件")


# ════════════════════════════════════════════════════════════════════════════
# 📊 ダッシュボード
# ════════════════════════════════════════════════════════════════════════════
if page == "📊 ダッシュボード":
    st.title("📊 ダッシュボード")

    df = get_confirmed_data()

    if df is None:
        st.info("確認済みデータがありません。「データ取込」からCSVをアップロードしてください。")
        st.subheader("📌 完成イメージ（サンプル）")
        import numpy as np
        rng = np.random.default_rng(42)
        months = pd.date_range("2024-01", periods=6, freq="ME")
        sample_rows = []
        for m in months:
            for store in st.session_state.stores:
                for plat in PLATFORMS:
                    sample_rows.append({"月": m.strftime("%Y-%m"), "店舗": store,
                                        "プラットフォーム": plat,
                                        "売上": int(rng.integers(50_000, 300_000))})
        df_s = pd.DataFrame(sample_rows)
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(df_s.groupby(["月", "店舗"])["売上"].sum().reset_index(),
                         x="月", y="売上", color="店舗", title="月別・店舗別売上（サンプル）")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig2 = px.pie(df_s.groupby("プラットフォーム")["売上"].sum().reset_index(),
                          values="売上", names="プラットフォーム", title="プラットフォーム別売上（サンプル）")
            st.plotly_chart(fig2, use_container_width=True)
        st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総売上",    fmt_yen(df["売上"].sum()))
    c2.metric("手数料合計", fmt_yen(df["手数料"].sum()))
    c3.metric("手取り合計", fmt_yen(df["手取り"].sum()))
    c4.metric("予約件数",  f"{len(df):,} 件")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(df.groupby(["月", "店舗"])["売上"].sum().reset_index(),
                     x="月", y="売上", color="店舗", title="月別・店舗別売上", barmode="group")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig2 = px.pie(df.groupby("プラットフォーム")["売上"].sum().reset_index(),
                      values="売上", names="プラットフォーム", title="プラットフォーム別売上")
        st.plotly_chart(fig2, use_container_width=True)

    fig3 = px.bar(df.groupby(["月", "プラットフォーム"])["売上"].sum().reset_index(),
                  x="月", y="売上", color="プラットフォーム", title="月別・プラットフォーム別売上")
    st.plotly_chart(fig3, use_container_width=True)

    if "決済方法" in df.columns:
        fig4 = px.pie(df.groupby("決済方法")["売上"].sum().reset_index(),
                      values="売上", names="決済方法", title="決済方法別売上")
        st.plotly_chart(fig4, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# 📥 データ取込
# ════════════════════════════════════════════════════════════════════════════
elif page == "📥 データ取込":
    st.title("📥 データ取込")
    st.caption("CSVをアップロードするとプラットフォームを自動判別します。重複は予約ID/決済IDで自動スキップ。")

    # ── CSV アップロード ──
    uploaded_files = st.file_uploader(
        "CSVファイルをアップロード（複数可）",
        type=["csv"],
        accept_multiple_files=True,
        key="csv_uploader",
    )

    if uploaded_files:
        for uploaded in uploaded_files:
            st.divider()
            st.subheader(f"📄 {uploaded.name}")

            df_raw = None
            for enc in ["utf-8-sig", "utf-8", "shift-jis", "cp932"]:
                for sep in [",", "\t"]:
                    try:
                        uploaded.seek(0)
                        tmp = pd.read_csv(uploaded, encoding=enc, sep=sep)
                        if len(tmp.columns) > 1:
                            df_raw = tmp
                            break
                    except Exception:
                        continue
                if df_raw is not None:
                    break

            if df_raw is None:
                st.error("読み込み失敗：文字コードを確認してください。")
                continue

            platform = detect_platform(df_raw.columns.tolist())
            if platform is None:
                st.error("プラットフォームを自動判別できませんでした。")
                st.caption("検出された列名:")
                st.code(", ".join(df_raw.columns.tolist()))
                continue

            schema = PLATFORM_SCHEMAS[platform]
            c1, c2 = st.columns([1, 2])
            with c1:
                st.success(f"✅ **{platform}** と判別")
                st.caption(f"{len(df_raw):,} 行")
                if schema.get("bank_transfer_keywords"):
                    st.info("⚠️ 銀行振込は取込後に手動確認が必要です")
            with c2:
                info = {
                    "利用日":  schema["usage_date"],
                    "店舗名":  schema["store"],
                    "売上":    schema["amount"],
                    "返金":    schema["refund"] or "なし",
                    "割引":    schema["discount"] or "なし",
                    "手取り":  schema["net_amount"] or "手数料率から計算",
                    "予約ID":  schema["booking_id"],
                    "顧客名":  schema["customer"],
                    "決済方法": schema.get("payment_method") or "なし",
                }
                st.dataframe(pd.DataFrame(info.items(), columns=["項目", "対応列"]),
                             hide_index=True, use_container_width=True)

            st.dataframe(df_raw.head(3), use_container_width=True)

            if st.button("✅ このCSVを取込む", key=f"import_{uploaded.name}"):
                new_cnt, dup_cnt = process_csv(df_raw, platform)
                if new_cnt > 0:
                    st.success(f"✅ {new_cnt:,} 件を登録（重複スキップ: {dup_cnt:,} 件）")
                else:
                    st.warning(f"新規データなし（全 {dup_cnt:,} 件が登録済み）")
                save_state()
                _rebuild_cache()
                st.rerun()

    # ── 銀行振込 入金確認 ──
    st.divider()
    st.subheader("🏦 銀行振込 入金確認")

    md = st.session_state.master_data
    if not md.empty and "確認済み" in md.columns:
        pending = md[(md["確認済み"] == False) & (md["データ種別"] == "CSV")]
        if pending.empty:
            st.success("未確認の銀行振込はありません")
        else:
            st.warning(f"入金確認待ち: {len(pending)} 件")
            show_cols = [c for c in ["予約ID", "月", "利用日", "顧客名", "店舗",
                                      "決済方法", "実売上"] if c in pending.columns]
            for idx, row in pending[show_cols + ["予約ID"]].iterrows():
                c1, c2, c3 = st.columns([3, 2, 1])
                with c1:
                    st.text(f"予約ID: {row.get('予約ID','')}  顧客: {row.get('顧客名','')}  店舗: {row.get('店舗','')}")
                with c2:
                    st.text(f"{row.get('月','')}  {fmt_yen(row.get('実売上', 0))}")
                with c3:
                    if st.button("✅ 入金確認", key=f"confirm_{idx}"):
                        st.session_state.master_data.at[idx, "確認済み"] = True
                        save_state()
                        _rebuild_cache()
                        st.rerun()
    else:
        st.info("CSVデータがまだありません")

    # ── 登録済みデータ一覧 ──
    st.divider()
    st.subheader("📋 登録済みデータ")
    if not md.empty:
        cnt_df = md.groupby(["プラットフォーム", "確認済み"]).size().reset_index(name="件数")
        st.dataframe(cnt_df, hide_index=True, use_container_width=True)
        show_cols = [c for c in ["予約ID", "月", "利用日", "プラットフォーム", "顧客名", "店舗",
                                  "決済方法", "売上", "割引", "返金", "実売上", "手取り", "確認済み"]
                     if c in md.columns]
        st.dataframe(md[show_cols].sort_values("月", ascending=False).head(20),
                     use_container_width=True)
        if st.button("🗑 全CSVデータをクリア"):
            st.session_state.master_data = pd.DataFrame()
            save_state()
            _rebuild_cache()
            st.rerun()
    else:
        st.info("CSVデータが登録されていません")


# ════════════════════════════════════════════════════════════════════════════
# 📄 請求書管理
# ════════════════════════════════════════════════════════════════════════════
elif page == "📄 請求書管理":
    st.title("📄 請求書管理（定期利用・銀行振込）")
    st.caption("定期利用者への請求書を店舗ごとに登録します。手数料なし・入金確認後に売上反映。")

    tab_input, tab_list = st.tabs(["➕ 請求書登録", "📋 請求書一覧・入金確認"])

    with tab_input:
        st.subheader("請求書を登録する")

        c1, c2, c3 = st.columns(3)
        with c1:
            inv_store   = st.selectbox("店舗", st.session_state.stores, key="inv_store")
        with c2:
            inv_month   = st.selectbox("対象月", MONTH_OPTIONS, key="inv_month")
        with c3:
            inv_number  = st.text_input("請求書番号", placeholder="例: INV-2026-001", key="inv_num")

        c4, c5 = st.columns(2)
        with c4:
            inv_customer = st.text_input("顧客名", key="inv_customer")
        with c5:
            inv_amount   = st.number_input("請求金額（円）", min_value=0, step=100, key="inv_amount")

        c6, c7 = st.columns(2)
        with c6:
            inv_date     = st.date_input("請求日", key="inv_date")
        with c7:
            inv_note     = st.text_input("備考（任意）", key="inv_note")

        if st.button("📄 請求書を登録", key="add_invoice"):
            if not inv_number or not inv_customer or inv_amount <= 0:
                st.error("請求書番号・顧客名・金額は必須です")
            else:
                new_row = pd.DataFrame([{
                    "請求書番号":     inv_number,
                    "月":             inv_month,
                    "利用日":         pd.Timestamp(inv_date),
                    "支払日":         pd.NaT,
                    "店舗":           inv_store,
                    "顧客名":         inv_customer,
                    "予約ID":         f"INV-{inv_number}",
                    "決済方法":       "銀行振込（請求書）",
                    "売上":           float(inv_amount),
                    "割引":           0.0,
                    "返金":           0.0,
                    "実売上":         float(inv_amount),
                    "入金額":         0.0,
                    "手数料率":       0.0,
                    "手数料":         0.0,
                    "手取り":         float(inv_amount),
                    "プラットフォーム": "請求書",
                    "データ種別":     "請求書",
                    "確認済み":       False,
                    "入金ステータス": "未入金",
                    "備考":           inv_note,
                }])
                existing_inv = st.session_state.invoice_data
                # 請求書番号で重複チェック
                if not existing_inv.empty and inv_number in existing_inv["請求書番号"].values:
                    st.error(f"請求書番号 {inv_number} は既に登録済みです")
                else:
                    if existing_inv.empty:
                        st.session_state.invoice_data = new_row
                    else:
                        st.session_state.invoice_data = pd.concat(
                            [existing_inv, new_row], ignore_index=True
                        )
                    st.success(f"✅ 請求書 {inv_number} を登録しました（入金確認後に売上反映）")
                    save_state()
                    _rebuild_cache()
                    st.rerun()

    with tab_list:
        inv = st.session_state.invoice_data
        # 既存データに新カラムがない場合はデフォルト値で補完
        if not inv.empty:
            if "入金額" not in inv.columns:
                inv["入金額"] = inv.apply(lambda r: r["実売上"] if r.get("確認済み", False) else 0.0, axis=1)
            if "入金ステータス" not in inv.columns:
                inv["入金ステータス"] = inv.apply(
                    lambda r: "入金済み" if r.get("確認済み", False) else "未入金", axis=1
                )
            st.session_state.invoice_data = inv

        if inv.empty:
            st.info("請求書データがありません。「請求書登録」タブから追加してください。")
        else:
            # ── 未入金一覧 ──
            pending_inv = inv[inv["確認済み"] == False]
            if not pending_inv.empty:
                st.subheader(f"🔴 入金未確認 ({len(pending_inv)} 件)")
                for idx, row in pending_inv.iterrows():
                    c1, c2, c3 = st.columns([3, 2, 2])
                    c1.write(f"**{row['請求書番号']}**　{row['顧客名']}")
                    c2.write(f"店舗: {row['店舗']}　月: {row['月']}")
                    c3.write(f"請求額: {fmt_yen(row['実売上'])}")

                    with st.expander(f"入金確認・編集　{row['請求書番号']}", expanded=False):
                        ec1, ec2 = st.columns(2)
                        with ec1:
                            deposit = st.number_input(
                                "入金額（円）", min_value=0, step=100,
                                value=int(row.get("入金額", 0)),
                                key=f"inv_dep_{idx}",
                            )
                        with ec2:
                            deposit_status = st.selectbox(
                                "入金ステータス",
                                ["未入金", "入金済み", "金額不一致", "未入金（督促済み）"],
                                index=0, key=f"inv_status_{idx}",
                            )
                        deposit_date = st.date_input("入金日", value=datetime.now().date(), key=f"inv_depdate_{idx}")

                        if st.button("✅ 入金確認して保存", key=f"inv_confirm_{idx}"):
                            st.session_state.invoice_data.at[idx, "入金額"] = float(deposit)
                            st.session_state.invoice_data.at[idx, "入金ステータス"] = deposit_status
                            st.session_state.invoice_data.at[idx, "支払日"] = pd.Timestamp(deposit_date)
                            if deposit_status == "入金済み":
                                st.session_state.invoice_data.at[idx, "確認済み"] = True
                                st.session_state.invoice_data.at[idx, "手取り"] = float(deposit)
                            else:
                                st.session_state.invoice_data.at[idx, "確認済み"] = False
                            save_state()
                            _rebuild_cache()
                            st.rerun()

            # ── 確認済み一覧 ──
            confirmed_inv = inv[inv["確認済み"] == True]
            if not confirmed_inv.empty:
                st.divider()
                st.subheader(f"🟢 入金確認済み ({len(confirmed_inv)} 件)")
                for idx, row in confirmed_inv.iterrows():
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                    c1.write(f"**{row['請求書番号']}**　{row['顧客名']}")
                    c2.write(f"店舗: {row['店舗']}　月: {row['月']}")
                    diff = row.get("入金額", row["実売上"]) - row["実売上"]
                    diff_text = f"（差額: {fmt_yen(diff)}）" if diff != 0 else ""
                    c3.write(f"請求: {fmt_yen(row['実売上'])} → 入金: {fmt_yen(row.get('入金額', row['実売上']))}{diff_text}")
                    with c4:
                        if st.button("↩️ 確認取消", key=f"inv_unconfirm_{idx}"):
                            st.session_state.invoice_data.at[idx, "確認済み"] = False
                            st.session_state.invoice_data.at[idx, "入金ステータス"] = "未入金"
                            st.session_state.invoice_data.at[idx, "支払日"] = pd.NaT
                            save_state()
                            _rebuild_cache()
                            st.rerun()

            # ── 請求書の編集 ──
            st.divider()
            st.subheader("✏️ 請求書を編集")
            edit_options = inv["請求書番号"].tolist()
            selected_inv = st.selectbox("編集する請求書を選択", edit_options, key="edit_inv_select")
            if selected_inv:
                edit_idx = inv[inv["請求書番号"] == selected_inv].index[0]
                edit_row = inv.loc[edit_idx]

                with st.form(key=f"edit_form_{edit_idx}"):
                    fc1, fc2, fc3 = st.columns(3)
                    with fc1:
                        new_store = st.selectbox("店舗", st.session_state.stores,
                                                 index=st.session_state.stores.index(edit_row["店舗"]) if edit_row["店舗"] in st.session_state.stores else 0)
                    with fc2:
                        new_month = st.selectbox("対象月", MONTH_OPTIONS,
                                                 index=MONTH_OPTIONS.index(edit_row["月"]) if edit_row["月"] in MONTH_OPTIONS else 0)
                    with fc3:
                        new_customer = st.text_input("顧客名", value=edit_row["顧客名"])

                    fc4, fc5 = st.columns(2)
                    with fc4:
                        new_amount = st.number_input("請求金額（円）", min_value=0, step=100, value=int(edit_row["実売上"]))
                    with fc5:
                        new_note = st.text_input("備考", value=edit_row.get("備考", "") or "")

                    if st.form_submit_button("💾 変更を保存"):
                        st.session_state.invoice_data.at[edit_idx, "店舗"] = new_store
                        st.session_state.invoice_data.at[edit_idx, "月"] = new_month
                        st.session_state.invoice_data.at[edit_idx, "顧客名"] = new_customer
                        st.session_state.invoice_data.at[edit_idx, "売上"] = float(new_amount)
                        st.session_state.invoice_data.at[edit_idx, "実売上"] = float(new_amount)
                        st.session_state.invoice_data.at[edit_idx, "手取り"] = float(new_amount) if not edit_row["確認済み"] else edit_row.get("入金額", float(new_amount))
                        st.session_state.invoice_data.at[edit_idx, "備考"] = new_note
                        save_state()
                        _rebuild_cache()
                        st.success("✅ 請求書を更新しました")
                        st.rerun()

            # ── 全請求書一覧テーブル ──
            st.divider()
            st.subheader("📋 全請求書一覧")
            show_cols = [c for c in ["請求書番号", "月", "店舗", "顧客名", "実売上", "入金額", "入金ステータス", "確認済み", "備考"]
                         if c in inv.columns]
            fmt_dict = {"実売上": fmt_yen}
            if "入金額" in inv.columns:
                fmt_dict["入金額"] = fmt_yen
            st.dataframe(
                inv[show_cols].style.format(fmt_dict),
                use_container_width=True,
            )

            st.divider()
            if st.button("🗑 全請求書データをクリア"):
                st.session_state.invoice_data = pd.DataFrame()
                save_state()
                _rebuild_cache()
                st.rerun()


# ════════════════════════════════════════════════════════════════════════════
# ⚙️ 設定
# ════════════════════════════════════════════════════════════════════════════
elif page == "⚙️ 設定":
    st.title("⚙️ 設定")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏢 店舗管理", "💳 手数料設定", "💰 固定費設定", "🌐 事業全体経費", "🔗 店舗名マッピング"])

    with tab1:
        st.subheader("店舗名の管理")
        stores_text = st.text_area("店舗名（1行1店舗）", value="\n".join(st.session_state.stores), height=150)
        if st.button("店舗名を保存", key="save_stores"):
            new_stores = [s.strip() for s in stores_text.splitlines() if s.strip()]
            st.session_state.stores = new_stores
            save_state()
            st.success(f"✅ {len(new_stores)} 店舗を登録しました")

    with tab2:
        st.subheader("プラットフォーム手数料率 (%)")
        new_fees = {}
        for plat in PLATFORMS:
            new_fees[plat] = st.number_input(
                plat, value=float(st.session_state.platform_fees.get(plat, 30)),
                min_value=0.0, max_value=100.0, step=0.1, key=f"fee_{plat}",
            )
        if st.button("手数料を保存", key="save_fees"):
            st.session_state.platform_fees = new_fees
            save_state()
            st.success("✅ 手数料率を保存しました")

    with tab3:
        st.subheader("月別固定費の入力")
        c1, c2 = st.columns(2)
        with c1:
            sel_store = st.selectbox("店舗を選択", st.session_state.stores, key="fc_store")
        with c2:
            sel_month = st.selectbox("月を選択", MONTH_OPTIONS, key="fc_month")

        cost_items = ["家賃", "光熱費", "人件費", "その他"]
        fc_key = f"{sel_store}_{sel_month}"
        existing_fc = st.session_state.fixed_costs.get(fc_key, {})
        cost_vals = {}
        for item in cost_items:
            cost_vals[item] = st.number_input(
                f"{item} (円)", value=float(existing_fc.get(item, 0)),
                min_value=0.0, step=1_000.0, format="%.0f",
                key=f"fc_{item}_{fc_key}",
            )
        if st.button("固定費を保存", key="save_fc"):
            st.session_state.fixed_costs[fc_key] = cost_vals
            save_state()
            st.success(f"✅ {sel_store} / {sel_month} の固定費を保存（合計: {fmt_yen(sum(cost_vals.values()))}）")

        if st.session_state.fixed_costs:
            st.divider()
            st.subheader("登録済み固定費一覧")
            fc_rows = []
            for k, v in st.session_state.fixed_costs.items():
                store_k, month_k = k.rsplit("_", 1)
                row = {"店舗": store_k, "月": month_k}
                row.update(v)
                row["合計"] = sum(v.values())
                fc_rows.append(row)
            st.dataframe(pd.DataFrame(fc_rows).sort_values(["店舗", "月"]), use_container_width=True)

    with tab4:
        st.subheader("事業全体の月別経費")
        st.caption("どの店舗にも属さない経費（プラットフォーム費・備品・LINE等）を月ごとに入力します。")

        sel_month_b = st.selectbox("月を選択", MONTH_OPTIONS, key="bc_month")
        bc_key = sel_month_b
        existing_bc = st.session_state.business_costs.get(bc_key, {})

        st.markdown("**経費項目を入力**")
        bc_items = ["よやクル月額費用", "備品費", "公式LINE", "広告費", "その他"]
        bc_vals = {}
        cols = st.columns(2)
        for i, item in enumerate(bc_items):
            with cols[i % 2]:
                bc_vals[item] = st.number_input(
                    f"{item} (円)",
                    value=float(existing_bc.get(item, 0)),
                    min_value=0.0, step=100.0, format="%.0f",
                    key=f"bc_{item}_{bc_key}",
                )

        st.markdown("**カスタム項目**")
        custom_label = st.text_input("項目名（独自）", key=f"bc_custom_label_{bc_key}", placeholder="例: 税理士費用")
        custom_val   = st.number_input("金額 (円)", min_value=0.0, step=100.0, format="%.0f", key=f"bc_custom_val_{bc_key}")
        if custom_label.strip():
            bc_vals[custom_label.strip()] = custom_val

        # 既存のカスタム項目を引き継ぐ
        for k, v in existing_bc.items():
            if k not in bc_vals:
                bc_vals[k] = v

        if st.button("事業全体経費を保存", key="save_bc"):
            st.session_state.business_costs[bc_key] = {k: v for k, v in bc_vals.items() if v > 0}
            save_state()
            st.success(f"✅ {sel_month_b} の事業全体経費を保存（合計: {fmt_yen(sum(bc_vals.values()))}）")

        if st.session_state.business_costs:
            st.divider()
            st.subheader("登録済み事業全体経費")
            bc_rows = []
            for m, v in sorted(st.session_state.business_costs.items()):
                row = {"月": m}
                row.update(v)
                row["合計"] = sum(v.values())
                bc_rows.append(row)
            st.dataframe(pd.DataFrame(bc_rows).fillna(0), use_container_width=True)

    with tab5:
        st.subheader("プラットフォーム別 店舗名マッピング")
        st.caption("CSV上の店舗名 → 正式店舗名 に変換します。")
        new_mapping = dict(st.session_state.store_mapping)
        for plat in PLATFORMS:
            st.markdown(f"**{plat}**")
            for store in st.session_state.stores:
                pname = st.text_input(
                    f"「{store}」に対応する {plat} 上の名前",
                    value=new_mapping.get(f"{plat}_{store}", ""),
                    key=f"map_{plat}_{store}",
                )
                if pname.strip():
                    new_mapping[f"{plat}_{pname.strip()}"] = store
        if st.button("マッピングを保存", key="save_mapping"):
            st.session_state.store_mapping = new_mapping
            save_state()
            st.success("✅ マッピングを保存しました")

        st.divider()
        if st.button("🔄 既存データにマッピングを再適用", key="remap_existing"):
            mapping = st.session_state.store_mapping
            def remap_df(df):
                if df.empty or "店舗" not in df.columns or "プラットフォーム" not in df.columns:
                    return df
                df = df.copy()
                df["店舗"] = df.apply(
                    lambda r: mapping.get(f"{r['プラットフォーム']}_{r['店舗']}", r["店舗"]), axis=1
                )
                return df
            st.session_state.master_data  = remap_df(st.session_state.master_data)
            st.session_state.invoice_data = remap_df(st.session_state.invoice_data)
            save_state()
            _rebuild_cache()
            st.success("✅ 既存データの店舗名を更新しました")
            st.rerun()
        if st.session_state.store_mapping:
            st.divider()
            rows = [{"プラットフォーム上の名前": k.split("_", 1)[1],
                     "プラットフォーム": k.split("_", 1)[0],
                     "正式店舗名": v}
                    for k, v in st.session_state.store_mapping.items()]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# 📈 損益レポート
# ════════════════════════════════════════════════════════════════════════════
elif page == "📈 損益レポート":
    st.title("📈 損益レポート")

    df = get_confirmed_data()
    if df is None:
        st.warning("確認済みデータがありません。データ取込・請求書管理から登録してください。")
        st.stop()

    pending_count = len(st.session_state.master_data[st.session_state.master_data["確認済み"] == False]) if not st.session_state.master_data.empty else 0
    if pending_count > 0:
        st.info(f"ℹ️ 銀行振込の入金未確認が {pending_count} 件あります。確認後に売上へ反映されます。")

    monthly = (
        df.groupby(["月", "店舗"])
        .agg(売上=("売上", "sum"), 割引=("割引", "sum"), 返金=("返金", "sum"),
             実売上=("実売上", "sum"), 手数料=("手数料", "sum"), 手取り=("手取り", "sum"),
             件数=("売上", "count"))
        .reset_index()
    )

    if st.session_state.fixed_costs:
        fc_rows = []
        for k, v in st.session_state.fixed_costs.items():
            store_k, month_k = k.rsplit("_", 1)
            fc_rows.append({"月": month_k, "店舗": store_k, "固定費": sum(v.values())})
        monthly = monthly.merge(pd.DataFrame(fc_rows), on=["月", "店舗"], how="left")
    else:
        monthly["固定費"] = 0

    monthly["固定費"]   = monthly["固定費"].fillna(0)
    monthly["営業利益"] = monthly["手取り"] - monthly["固定費"]

    total = (
        monthly.groupby("月")
        .agg(売上=("売上", "sum"), 割引=("割引", "sum"), 返金=("返金", "sum"),
             実売上=("実売上", "sum"), 手数料=("手数料", "sum"), 手取り=("手取り", "sum"),
             固定費=("固定費", "sum"), 件数=("件数", "sum"))
        .reset_index()
    )

    # 事業全体経費を月別に集計してtotalにマージ
    if st.session_state.business_costs:
        bc_rows = [{"月": m, "事業経費": sum(v.values())}
                   for m, v in st.session_state.business_costs.items()]
        total = total.merge(pd.DataFrame(bc_rows), on="月", how="left")
    else:
        total["事業経費"] = 0
    total["事業経費"] = total["事業経費"].fillna(0)
    total["営業利益"] = total["手取り"] - total["固定費"] - total["事業経費"]

    tab_all, *tab_stores = st.tabs(["🌐 事業全体"] + st.session_state.stores)

    fmt_cols = {col: fmt_yen for col in ["売上", "割引", "返金", "実売上", "手数料", "手取り", "固定費", "事業経費", "営業利益"]}

    def show_table(df_view: pd.DataFrame):
        valid_fmt = {k: v for k, v in fmt_cols.items() if k in df_view.columns}
        styled = df_view.style.format(valid_fmt, na_rep="-")
        if "営業利益" in df_view.columns:
            styled = styled.applymap(color_profit, subset=["営業利益"])
        st.dataframe(styled, use_container_width=True)

    def pl_chart(df_view: pd.DataFrame, title: str, show_business_cost: bool = False):
        fig = go.Figure()
        fig.add_bar(x=df_view["月"], y=df_view["手取り"], name="手取り", marker_color="#4C9BE8")
        fig.add_bar(x=df_view["月"], y=df_view["固定費"], name="店舗固定費", marker_color="#F28B82")
        if show_business_cost and "事業経費" in df_view.columns:
            fig.add_bar(x=df_view["月"], y=df_view["事業経費"], name="事業全体経費", marker_color="#FBBC04")
        fig.add_scatter(x=df_view["月"], y=df_view["営業利益"], name="営業利益",
                        mode="lines+markers", line=dict(color="#34A853", width=2))
        fig.update_layout(title=title, barmode="group", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

    with tab_all:
        st.subheader("事業全体 損益サマリー")
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("累計売上",     fmt_yen(total["売上"].sum()))
        c2.metric("累計返金",     fmt_yen(total["返金"].sum()))
        c3.metric("累計実売上",   fmt_yen(total["実売上"].sum()))
        c4.metric("累計店舗固定費", fmt_yen(total["固定費"].sum()))
        c5.metric("累計事業経費", fmt_yen(total["事業経費"].sum()))
        profit_all = total["営業利益"].sum()
        c6.metric("累計営業利益", fmt_yen(profit_all),
                  delta=("黒字" if profit_all >= 0 else "赤字"),
                  delta_color=("normal" if profit_all >= 0 else "inverse"))
        show_table(total[["月", "売上", "割引", "返金", "実売上", "手数料", "手取り", "固定費", "事業経費", "営業利益", "件数"]])
        pl_chart(total, "事業全体 月別損益推移", show_business_cost=True)
        fig_cmp = px.bar(monthly, x="月", y="営業利益", color="店舗",
                         title="店舗別 営業利益比較", barmode="group")
        st.plotly_chart(fig_cmp, use_container_width=True)

    for i, store in enumerate(st.session_state.stores):
        with tab_stores[i]:
            st.subheader(f"{store} 損益")
            sd = monthly[monthly["店舗"] == store].copy()
            if sd.empty:
                st.info(f"{store} のデータはまだありません。")
                continue
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("累計売上",   fmt_yen(sd["売上"].sum()))
            c2.metric("累計返金",   fmt_yen(sd["返金"].sum()))
            c3.metric("累計実売上", fmt_yen(sd["実売上"].sum()))
            p = sd["営業利益"].sum()
            c4.metric("累計営業利益", fmt_yen(p),
                      delta=("黒字" if p >= 0 else "赤字"),
                      delta_color=("normal" if p >= 0 else "inverse"))
            show_table(sd[["月", "売上", "割引", "返金", "実売上", "手数料", "手取り", "固定費", "営業利益", "件数"]])
            pl_chart(sd, f"{store} 月別損益推移")

    st.divider()
    if st.button("📥 Excelレポートを生成"):
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            total.to_excel(writer,   sheet_name="事業全体損益", index=False)
            monthly.to_excel(writer, sheet_name="店舗別損益",   index=False)
            df.to_excel(writer,      sheet_name="明細データ",   index=False)
        st.download_button(
            label="📊 Excelをダウンロード",
            data=buf.getvalue(),
            file_name=f"studio_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ════════════════════════════════════════════════════════════════════════════
# 👥 顧客分析
# ════════════════════════════════════════════════════════════════════════════
elif page == "👥 顧客分析":
    st.title("👥 顧客分析")

    df = get_confirmed_data()
    if df is None or "顧客名" not in df.columns:
        st.warning("データがありません。")
        st.stop()

    df_c = df[df["顧客名"].notna() & ~df["顧客名"].isin(["不明", ""])].copy()
    if df_c.empty:
        st.warning("顧客名のデータがありません。")
        st.stop()

    cust_stats = (
        df_c.groupby("顧客名")
        .agg(利用回数=("予約ID", "count"), 累計売上=("実売上", "sum"),
             利用店舗数=("店舗", "nunique"), 最終利用日=("利用日", "max"),
             初回利用日=("利用日", "min"))
        .reset_index()
        .sort_values("利用回数", ascending=False)
    )
    cust_stats["リピーター"] = cust_stats["利用回数"] > 1
    cust_stats["平均単価"]   = (cust_stats["累計売上"] / cust_stats["利用回数"]).round(0)

    total_customers  = len(cust_stats)
    repeat_customers = cust_stats["リピーター"].sum()
    repeat_rate      = repeat_customers / total_customers * 100 if total_customers else 0
    avg_visits       = cust_stats["利用回数"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総顧客数",     f"{total_customers:,} 人")
    c2.metric("リピーター数", f"{repeat_customers:,} 人")
    c3.metric("リピート率",   f"{repeat_rate:.1f} %")
    c4.metric("平均利用回数", f"{avg_visits:.1f} 回")

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["📊 ランキング", "📅 月別推移", "🗺️ 利用傾向", "📋 顧客一覧"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            top_v = cust_stats.nlargest(20, "利用回数")
            fig = px.bar(top_v, x="利用回数", y="顧客名", orientation="h",
                         color="利用回数", color_continuous_scale="Blues", title="利用回数 TOP20")
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            top_s = cust_stats.nlargest(20, "累計売上")
            fig2 = px.bar(top_s, x="累計売上", y="顧客名", orientation="h",
                          color="累計売上", color_continuous_scale="Greens", title="累計売上 TOP20")
            fig2.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        first_visit = df_c.groupby("顧客名")["月"].min().rename("初回月")
        df_c2 = df_c.merge(first_visit, on="顧客名")
        df_c2["顧客区分"] = df_c2.apply(lambda r: "新規" if r["月"] == r["初回月"] else "リピーター", axis=1)
        monthly_type = (
            df_c2.groupby(["月", "顧客区分"])
            .agg(人数=("顧客名", "nunique"), 売上=("実売上", "sum"))
            .reset_index()
        )
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(monthly_type, x="月", y="人数", color="顧客区分",
                         title="月別 新規/リピーター 人数",
                         color_discrete_map={"新規": "#4C9BE8", "リピーター": "#34A853"})
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig2 = px.bar(monthly_type, x="月", y="売上", color="顧客区分",
                          title="月別 新規/リピーター 売上",
                          color_discrete_map={"新規": "#4C9BE8", "リピーター": "#34A853"})
            st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.histogram(cust_stats, x="利用回数", nbins=20, title="利用回数の分布",
                               color_discrete_sequence=["#4C9BE8"])
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            store_cust = df_c.groupby("店舗")["顧客名"].nunique().reset_index().rename(columns={"顧客名": "顧客数"})
            fig2 = px.bar(store_cust, x="店舗", y="顧客数", title="店舗別 利用顧客数", color="店舗")
            st.plotly_chart(fig2, use_container_width=True)

        if "利用日" in df_c.columns:
            df_c["曜日"] = pd.to_datetime(df_c["利用日"], errors="coerce").dt.day_name()
            weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            weekday_jp    = {"Monday":"月","Tuesday":"火","Wednesday":"水","Thursday":"木",
                             "Friday":"金","Saturday":"土","Sunday":"日"}
            wk = df_c.groupby("曜日").size().reindex(weekday_order).reset_index(name="件数")
            wk["曜日JP"] = wk["曜日"].map(weekday_jp)
            fig3 = px.bar(wk, x="曜日JP", y="件数", title="曜日別 利用件数",
                          color="件数", color_continuous_scale="Oranges")
            st.plotly_chart(fig3, use_container_width=True)

    with tab4:
        search = st.text_input("顧客名で検索", placeholder="名前を入力...")
        filtered = cust_stats[cust_stats["顧客名"].str.contains(search, na=False)] if search else cust_stats
        st.dataframe(filtered.style.format({"累計売上": fmt_yen, "平均単価": fmt_yen}),
                     use_container_width=True)
        st.divider()
        st.subheader("顧客詳細")
        sel_cust = st.selectbox("顧客を選択", cust_stats["顧客名"].tolist())
        if sel_cust:
            cust_detail = df_c[df_c["顧客名"] == sel_cust].sort_values("利用日", ascending=False)
            detail_cols = [c for c in ["予約ID", "利用日", "店舗", "プラットフォーム", "決済方法",
                                        "売上", "割引", "返金", "実売上"] if c in cust_detail.columns]
            st.dataframe(cust_detail[detail_cols], use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# 🔍 予約検索
# ════════════════════════════════════════════════════════════════════════════
elif page == "🔍 予約検索":
    st.title("🔍 予約検索")

    df = get_all_data()
    if df is None:
        st.warning("データがありません。")
        st.stop()

    st.subheader("予約IDで検索")
    search_id = st.text_input("予約IDを入力", placeholder="例: o_PaRYwq0hS3yS0TqWPLCbzg")

    if search_id and "予約ID" in df.columns:
        result = df[df["予約ID"].astype(str).str.contains(search_id, case=False, na=False)]
        if result.empty:
            st.warning(f"「{search_id}」に該当する予約が見つかりませんでした。")
        else:
            st.success(f"{len(result)} 件 見つかりました")
            show_cols = [c for c in ["予約ID", "利用日", "顧客名", "店舗", "プラットフォーム",
                                      "決済方法", "売上", "割引", "返金", "実売上", "手取り", "確認済み"]
                         if c in result.columns]
            st.dataframe(result[show_cols], use_container_width=True)
            if "返金" in result.columns and result["返金"].sum() > 0:
                st.warning(f"⚠️ 返金あり（合計: {fmt_yen(result['返金'].sum())}）")

    st.divider()
    st.subheader("返金あり 予約一覧")
    if "返金" in df.columns:
        refunded = df[df["返金"] > 0].sort_values("利用日", ascending=False)
        if refunded.empty:
            st.info("返金のある予約はありません。")
        else:
            st.caption(f"返金あり件数: {len(refunded):,} 件　合計返金額: {fmt_yen(refunded['返金'].sum())}")
            show_cols = [c for c in ["予約ID", "利用日", "顧客名", "店舗", "プラットフォーム",
                                      "決済方法", "売上", "返金", "実売上"] if c in refunded.columns]
            st.dataframe(refunded[show_cols], use_container_width=True)
