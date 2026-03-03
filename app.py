from flask import Flask, request, jsonify, send_file, render_template
import pandas as pd
import re
import io

app = Flask(__name__)

# ─────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────

def normalize_phone(raw):
    if pd.isna(raw) or str(raw).strip() == "":
        return None
    s = str(raw).strip()
    if re.search(r"[eE]", s):
        try:
            s = str(int(float(s)))
        except:
            pass
    digits = re.sub(r"\D", "", s)
    return digits[-10:] if len(digits) >= 10 else None


def find_col(columns, keywords):
    for kw in keywords:
        for col in columns:
            if kw.lower() in col.strip().lower():
                return col
    return None


def load_df(file):
    name = file.filename.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str)
    elif name.endswith((".xlsx", ".xls")):
        return pd.read_excel(file, dtype=str)
    else:
        raise ValueError("Unsupported file format. Upload CSV or Excel.")


def clean_gupshup(df):
    cols = df.columns.tolist()
    phone_col   = next((c for c in cols if c.strip() == "MOBILE"), None) \
               or find_col(cols, ["mobile", "phone", "contact", "msisdn"])
    content_col = next((c for c in cols if c.strip() == "CONTENT"), None) \
               or find_col(cols, ["content", "message", "msg", "text", "body"])

    if not phone_col:
        raise ValueError("Could not find phone column in Gupshup file.")

    df["phone_clean"] = df[phone_col].apply(normalize_phone)
    df["content"]     = df[content_col].fillna("").str.strip() if content_col else ""
    df["phone_raw"]   = df[phone_col]

    cleaned = df.dropna(subset=["phone_clean"])[["phone_clean", "phone_raw", "content"]].copy()
    return cleaned


GUPSHUP_SOURCES = {
    "Non standard prompt",
    "Find a Store – Rq",
    "Find a Store – Store Found",
    "Find a Store – No Store Found",
    "Desk Digital Leads",
    "Chatbot - Live Offers",
    "Chatbot - Product Enquiry",
    "Chatbot - Sales Enquiry",
    "Chair Digital Leads 50% off",
    "Chair Digital Leads",
    "Find a Store – No Response",
    "Chatbot - No Response",
    "Whatsapp map location",
    "Chair Recommender - WhatsApp",
    "Work from store",
    "Find Store CTWA",
    "Store Visit NPS-Good",
    "Recliner Recommender - WhatsApp",
    "Mattress Recommender - WhatsApp",
    "Chatbot - Explore Website",
    "Desk Digital Leads 50% off",
    "Store Visit NPS-Excellent",
    "Warranty Registration",
    "Explore Products",
    "Appointment Booked",
    "Next Day Delivery",
    "Amazon Review Given - QR",
    "Sofa Price Drop 35% Off",
    "Amazon Review Qr Scanned",
    "Chatbot - Check out Products",
    "Free Physio Consultation",
    "Pillow Exchange Offer",
    "Call Us",
    "Store Visit NPS-Poor",
    "Sofa Price Drop 55% Off",
    "Store Visit NPS-Poor reason",
    "Pillow Recommender - WhatsApp",
    "Warmup Leads",
    "Whatsapp push clicked",
    "Chatbot - Video Shopping Demo",
    "New home buyer - yes",
    "Chatbot - Store Information",
    "Consultation - Appointment Booked",
    "Mattress Digital Leads 40% Off",
    "Mattress Education",
    "Agent Assist - WhatsApp",
    "Store Page - Book a Visit",
    "Amazon Review Given - WhatsApp Push",
    "Pillow Exchange Offer - No Response",
    "Price Drop 45% Off Sale",
}


def clean_lsq(df):
    cols = df.columns.tolist()
    phone_col  = next((c for c in cols if c.strip() == "Phone Number"), None) \
              or find_col(cols, ["phone", "mobile", "contact", "msisdn"])
    source_col = next((c for c in cols if c.strip() == "Source"), None) \
              or find_col(cols, ["source", "lead source", "channel", "origin"])

    if not phone_col:
        raise ValueError("Could not find phone column in LSQ file.")
    if not source_col:
        raise ValueError("Could not find Source column in LSQ file.")

    # Normalize source for matching (strip whitespace)
    df["_source_clean"] = df[source_col].astype(str).str.strip()

    # ── KEY FILTER: keep only Gupshup sources ──
    before = len(df)
    df = df[df["_source_clean"].isin(GUPSHUP_SOURCES)].copy()
    after = len(df)

    print(f"   LSQ rows before source filter : {before:,}")
    print(f"   LSQ rows after  source filter : {after:,}  (kept Gupshup sources only)")

    df["phone_clean"] = df[phone_col].apply(normalize_phone)
    return df.dropna(subset=["phone_clean"])[["phone_clean"]].copy()


def reconcile(g_df, l_df, unique_mode):
    lsq_set = set(l_df["phone_clean"].unique())
    if unique_mode:
        g_df = g_df.drop_duplicates(subset=["phone_clean"])
    leaked  = g_df[~g_df["phone_clean"].isin(lsq_set)].copy()
    matched = g_df[ g_df["phone_clean"].isin(lsq_set)].copy()
    return leaked, matched


def build_pivot(leaked_df):
    """Pivot: prompt text → leak count, sorted desc."""
    pivot = (
        leaked_df["content"]
        .value_counts()
        .reset_index()
    )
    pivot.columns = ["Prompt / Content", "Leak Count"]
    pivot = pivot.sort_values("Leak Count", ascending=False).reset_index(drop=True)
    pivot.insert(0, "Rank", range(1, len(pivot) + 1))
    return pivot


# ─────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyse", methods=["POST"])
def analyse():
    try:
        g_file      = request.files.get("gupshup")
        l_file      = request.files.get("lsq")
        unique_mode = request.form.get("unique_mode", "false") == "true"

        if not g_file or not l_file:
            return jsonify({"error": "Both files are required."}), 400

        g_df = clean_gupshup(load_df(g_file))
        l_df = clean_lsq(load_df(l_file))

        leaked, matched = reconcile(g_df, l_df, unique_mode)

        total_g      = len(g_df)
        unique_g     = g_df["phone_clean"].nunique()
        total_l      = len(l_df)          # already filtered to Gupshup sources
        unique_l     = l_df["phone_clean"].nunique()
        base         = unique_g if unique_mode else total_g
        leaked_count = len(leaked)
        matched_count= len(matched)
        leakage_pct  = round(leaked_count / base * 100, 2) if base else 0

        # Build pivot and store as Excel in memory
        pivot = build_pivot(leaked)
        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            pivot.to_excel(writer, sheet_name="Leaked by Prompt", index=False)

            # Auto-size columns
            ws = writer.sheets["Leaked by Prompt"]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 80)

        app.config["LEAKED_EXCEL"] = excel_buf.getvalue()

        return jsonify({
            "success": True,
            "summary": {
                "total_gupshup":  total_g,
                "unique_gupshup": unique_g,
                "total_lsq":      total_l,
                "unique_lsq":     unique_l,
                "leaked":         leaked_count,
                "matched":        matched_count,
                "leakage_pct":    leakage_pct,
                "mode":           "Unique Users" if unique_mode else "All Messages"
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/download")
def download():
    data = app.config.get("LEAKED_EXCEL")
    if not data:
        return "No data. Run analysis first.", 400
    buf = io.BytesIO(data)
    buf.seek(0)
    return send_file(buf,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True,
                     download_name="leaked_records.xlsx")


if __name__ == "__main__":
    app.run(debug=True)