# app.py

from flask import Flask, render_template, abort, request, redirect, url_for, flash, jsonify, send_file
import sqlite3
import csv
import os
import io

from datetime import datetime, date, timedelta
import pandas as pd
from io import BytesIO
from dateutil import parser

import smtplib
from email.message import EmailMessage

app = Flask(__name__)
app.secret_key = "super_secret_key"

# ---- Email config (fill with real values) ----
SMTP_SERVER = "smtp.gmail.com"     # ya jo bhi server use karte ho
SMTP_PORT = 587                     # Gmail = 587 (TLS)
SENDER_EMAIL = "sonukumar6396727@gmail.com"
SENDER_PASSWORD = "ihbihglefnfwpvlg"   # app password / SMTP password
MANAGER_EMAIL = "sonu.kumar@qtsolv.com"

DB_NAME = "gmc_data.db"


# ===== Helpdesk Report Utilities (for /helpdesk_report) =====

def load_file(path_or_fileobj):
    if hasattr(path_or_fileobj, "read"):
        try:
            return pd.read_csv(path_or_fileobj, dtype=str)
        except Exception:
            path_or_fileobj.seek(0)
            return pd.read_excel(path_or_fileobj, dtype=str)
    path = str(path_or_fileobj)
    if path.lower().endswith(".csv"):
        return pd.read_csv(path, dtype=str)
    return pd.read_excel(path, dtype=str)


def find_col(df, names):
    lc = {c.lower(): c for c in df.columns}
    for n in names:
        if n in lc:
            return lc[n]
    return None


def safe_parse_dt(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    try:
        t = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if not pd.isna(t):
            return t.to_pydatetime()
    except Exception:
        pass
    try:
        return parser.parse(s, dayfirst=True, fuzzy=True)
    except Exception:
        return None


def compute_buckets(df, col_created, col_updated):
    df = df.copy()
    df['_created_dt'] = df[col_created].apply(safe_parse_dt)
    df['_updated_dt'] = df[col_updated].apply(safe_parse_dt)
    df['_cdate'] = df['_created_dt'].apply(lambda x: x.date() if x is not None else None)
    df['_udate'] = df['_updated_dt'].apply(lambda x: x.date() if x is not None else None)

    def diff_days(row):
        c = row['_cdate']
        u = row['_udate']
        if c is None or u is None:
            return None
        if u < c:
            return 0
        return (u - c).days

    df['_diff'] = df.apply(diff_days, axis=1)

    def bucket(d):
        if d is None:
            return ''
        if d == 0:
            return '0'
        if d == 1 or d == 2:
            return '2'
        if d > 2:
            return '2+'
        return ''

    df['Group_Days'] = df['_diff'].apply(bucket)
    return df


def build_payload_from_df(df, id_col, status_col, dept_col):
    cols = ['0', '2', '2+']
    df[status_col] = df[status_col].astype(str).str.strip()
    df[dept_col] = df[dept_col].astype(str).str.strip()

    grouped = df.groupby([status_col, dept_col, 'Group_Days'])[id_col].count().reset_index(name='count')

    status_order = list(df[status_col].astype(str).unique())
    dept_order = list(df[dept_col].astype(str).unique())

    lookup = {(r[status_col], r[dept_col], r['Group_Days']): r['count'] for _, r in grouped.iterrows()}

    rows = []
    status_matrix = {}
    dept_matrix = {}
    grand_totals = {c: 0 for c in cols}
    grand_total = 0

    for status in status_order:
        for dept in dept_order:
            if not ((df[status_col] == status) & (df[dept_col] == dept)).any():
                continue
            counts = {c: int(lookup.get((status, dept, c), 0)) for c in cols}
            gtotal = sum(counts.values())
            rows.append({'status': status, 'department': dept, 'counts': counts, 'grand_total': gtotal})
            status_matrix.setdefault(status, {})
            dept_matrix.setdefault(dept, {})
            for c in cols:
                status_matrix[status][c] = status_matrix[status].get(c, 0) + counts[c]
                dept_matrix[dept][c] = dept_matrix[dept].get(c, 0) + counts[c]
                grand_totals[c] += counts[c]
                grand_total += counts[c]

    status_totals = []
    for s in status_order:
        counts = status_matrix.get(s, {c: 0 for c in cols})
        status_totals.append({'status': s, 'counts': counts, 'grand_total': int(sum(counts.values()))})

    # department-wise open tickets
    open_mask = df[status_col].str.lower() == 'open'
    if id_col in df.columns:
        open_series = df[open_mask].groupby(dept_col)[id_col].nunique()
    else:
        open_series = df[open_mask].groupby(dept_col).size()

    open_by_dept = [
        {'department': dept, 'count': int(val)}
        for dept, val in open_series.items()
    ]

    summary = {
        'total': int(df[id_col].nunique()) if id_col in df.columns else int(len(df)),
        'closed': int((df[status_col].str.lower() == 'closed').sum()),
        'inprogress': int(df[status_col].str.lower().str.contains('in progress').sum()),
        'open': int((df[status_col].str.lower() == 'open').sum())
    }

    payload = {
        'columns': cols,
        'rows': rows,
        'status_totals': status_totals,
        'grand_totals': grand_totals,
        'grand_total': grand_total,
        'summary': summary,
        'status_order': status_order,
        'dept_order': dept_order,
        'status_matrix': status_matrix,
        'dept_matrix': dept_matrix,
        'open_by_dept': open_by_dept
    }
    return payload


def build_excel_df(payload):
    """
    Excel sheet:
    Closed → Closed Total → In progress → In progress Total → Open → Open Total → Grand Total
    - Status only on first detail row of each group
    - 0 values shown as blank ("")
    """
    cols = payload['columns']
    rows = payload['rows']
    status_totals = payload['status_totals']
    grand_totals = payload['grand_totals']
    grand_total = payload['grand_total']
    status_order = payload.get('status_order', [])

    def disp(v):
        if v is None:
            return ""
        try:
            return "" if int(v) == 0 else int(v)
        except Exception:
            return v

    rows_by_status = {}
    for r in rows:
        rows_by_status.setdefault(r['status'], []).append(r)

    priority = ["Closed", "In progress", "Open"]
    ordered_status = []
    for p in priority:
        if p in status_order and p not in ordered_status:
            ordered_status.append(p)
    for s in status_order:
        if s not in ordered_status:
            ordered_status.append(s)

    totals_by_status = {st["status"]: st for st in status_totals}

    excel_rows = []

    for status in ordered_status:
        detail_rows = rows_by_status.get(status, [])
        first = True
        for r in detail_rows:
            c0 = r["counts"].get("0", 0)
            c2 = r["counts"].get("2", 0)
            c2p = r["counts"].get("2+", 0)
            gt = r["grand_total"]

            excel_rows.append({
                "Status": status if first else "",
                "Department": r["department"],
                "0": disp(c0),
                "2": disp(c2),
                "2+": disp(c2p),
                "Grand Total": disp(gt),
            })
            first = False

        st = totals_by_status.get(status)
        if st is not None:
            t0 = st["counts"].get("0", 0)
            t2 = st["counts"].get("2", 0)
            t2p = st["counts"].get("2+", 0)
            tgt = st["grand_total"]

            excel_rows.append({
                "Status": f"{status} Total",
                "Department": "",
                "0": disp(t0),
                "2": disp(t2),
                "2+": disp(t2p),
                "Grand Total": disp(tgt),
            })

    g0 = grand_totals.get("0", 0)
    g2 = grand_totals.get("2", 0)
    g2p = grand_totals.get("2+", 0)

    excel_rows.append({
        "Status": "Grand Total",
        "Department": "",
        "0": disp(g0),
        "2": disp(g2),
        "2+": disp(g2p),
        "Grand Total": disp(grand_total),
    })

    return pd.DataFrame(excel_rows)


# ===== Report history helpers (Level 1) =====

def ensure_report_history_table():
    """Create report_history table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_date TEXT NOT NULL,
            end_date TEXT,
            file_name TEXT,
            sent_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def get_last_report_range():
    """Return (start_date, end_date) of last sent report, or (None, None)."""
    ensure_report_history_table()
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT start_date, end_date
        FROM report_history
        ORDER BY id DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None, None


def save_report_history(start_date, end_date, file_name):
    """Insert a new row in report_history."""
    ensure_report_history_table()
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    sent_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        INSERT INTO report_history (start_date, end_date, file_name, sent_at)
        VALUES (?, ?, ?, ?)
    """, (start_date, end_date, file_name, sent_at))
    conn.commit()
    conn.close()


def send_report_email(excel_bytes, start_date, end_date):
    """Send the Excel report via email to manager."""
    msg = EmailMessage()
    if start_date and end_date:
        subject_range = f"{start_date} to {end_date}"
    elif start_date:
        subject_range = f"from {start_date}"
    else:
        subject_range = "latest"

    msg['Subject'] = f"Weekly Helpdesk Report ({subject_range})"
    msg['From'] = SENDER_EMAIL
    msg['To'] = MANAGER_EMAIL

    body = f"""Dear Manager,

Please find attached the helpdesk ticket report for {subject_range}.

This report was generated automatically from the Helpdesk Portal.

Regards,
Automated Report System
"""
    msg.set_content(body)

    msg.add_attachment(
        excel_bytes,
        maintype='application',
        subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        filename='ticket_report.xlsx'
    )

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)


# globals for Helpdesk
LAST_PIVOT_DF = None        # Excel-friendly table
LAST_DF2 = None             # processed df with buckets
LAST_META = {}              # id/status/dept col names
LAST_RANGE = None           # {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
LAST_FILE_NAME = None       # last uploaded file name


# ----------------- Helpers -----------------
def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def parse_number(value):
    if value is None:
        return 0
    s = str(value).replace(",", "").strip()
    if not s:
        return 0
    try:
        return float(s)
    except ValueError:
        return 0


# ---------- Age helpers (used to compute fallback age/age_range) ----------
def calculate_age_from_dob(dob_str):
    """Return integer age or None if dob_str can't be parsed."""
    if not dob_str:
        return None
    dob_str = str(dob_str).strip()
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y"]
    for fmt in formats:
        try:
            dob = datetime.strptime(dob_str, fmt).date()
            today = date.today()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            return age
        except Exception:
            continue
    return None


def age_range_from_age(age):
    """Return age band string from integer age; empty string for None."""
    if age is None or age == "":
        return ""
    try:
        age = int(age)
    except Exception:
        return ""
    if age < 18:
        return "<18"
    if age <= 25:
        return "18-25"
    if age <= 35:
        return "26-35"
    if age <= 45:
        return "36-45"
    if age <= 60:
        return "46-60"
    return "61+"


# ----------------- DB init -----------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # table for form submissions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gmc_forms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT,
            name TEXT,
            last_modified_time TEXT,
            employee_code TEXT,
            employee_name_aadhar TEXT,
            employee_gender TEXT,
            employee_dob_aadhar TEXT,
            dep1_name TEXT,
            dep1_relation TEXT,
            dep1_gender TEXT,
            dep1_dob TEXT,
            dep2_name TEXT,
            dep2_relation TEXT,
            dep2_gender TEXT,
            dep2_dob TEXT,
            dep3_name TEXT,
            dep3_relation TEXT,
            dep3_gender TEXT,
            dep3_dob TEXT,
            dep4_name TEXT,
            dep4_relation TEXT,
            dep4_gender TEXT,
            dep4_dob TEXT,
            dep5_name TEXT,
            dep5_relation TEXT,
            dep5_gender TEXT,
            dep5_dob TEXT
        )
    """)

    # master table for GMC data (from CSV)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gmc_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_code TEXT,
            name TEXT,
            official_dob TEXT,
            relation TEXT,
            gender TEXT,
            sum_assured TEXT,
            age TEXT,
            "range" TEXT,
            total_premium REAL,
            monthly_premium REAL
        )
    """)

    conn.commit()
    conn.close()


# Dummy products for /store
products = [
    {"id": 1, "name": "Wireless Headphones", "price": "₹1,499", "tagline": "Up to 20 Hours Playtime",
     "description": "Comfortable over-ear wireless headphones with deep bass and long battery life."},
    {"id": 2, "name": "Smartphone 5G", "price": "₹19,999", "tagline": "6GB RAM, 128GB Storage",
     "description": "Powerful 5G smartphone with great camera and fast performance."},
    {"id": 3, "name": "Gaming Mouse", "price": "₹799", "tagline": "RGB Lights, 6 Buttons",
     "description": "Ergonomic gaming mouse with customizable DPI and RGB lighting."},
    {"id": 4, "name": "Laptop 15.6 inch", "price": "₹45,999", "tagline": "i5, 8GB RAM, 512GB SSD",
     "description": "Lightweight laptop suitable for work, study and entertainment."},
    {"id": 5, "name": "Bluetooth Speaker", "price": "₹1,099", "tagline": "Loud & Clear Bass",
     "description": "Portable speaker with clear sound and strong bass."},
    {"id": 6, "name": "Smart Watch", "price": "₹2,499", "tagline": "Heart Rate, SpO2, Steps",
     "description": "Fitness smart watch with multiple sports modes and notifications."},
]


# ====== Routes ======
@app.route("/")
def home():
    return render_template("home.html")


@app.route("/store")
def store():
    return render_template("index.html", products=products)


@app.route("/product/<int:product_id>")
def product_detail(product_id):
    product = next((p for p in products if p["id"] == product_id), None)
    if product is None:
        abort(404)
    return render_template("product_detail.html", product=product)


# GMC Submission Form
@app.route("/gmc", methods=["GET", "POST"])
def gmc_form():
    if request.method == "POST":
        # Basic details
        email = request.form.get("email")
        name = request.form.get("name")
        last_modified_time = request.form.get("last_modified_time")
        employee_code = request.form.get("employee_code")

        # Employee details
        employee_name_aadhar = request.form.get("employee_name_aadhar")
        employee_gender = request.form.get("employee_gender")
        employee_dob_aadhar = request.form.get("employee_dob_aadhar")

        # Dependent 1
        dep1_name = request.form.get("dep1_name")
        dep1_relation = request.form.get("dep1_relation")
        dep1_gender = request.form.get("dep1_gender")
        dep1_dob = request.form.get("dep1_dob")

        # Dependent 2
        dep2_name = request.form.get("dep2_name")
        dep2_relation = request.form.get("dep2_relation")
        dep2_gender = request.form.get("dep2_gender")
        dep2_dob = request.form.get("dep2_dob")

        # Dependent 3
        dep3_name = request.form.get("dep3_name")
        dep3_relation = request.form.get("dep3_relation")
        dep3_gender = request.form.get("dep3_gender")
        dep3_dob = request.form.get("dep3_dob")

        # Dependent 4
        dep4_name = request.form.get("dep4_name")
        dep4_relation = request.form.get("dep4_relation")
        dep4_gender = request.form.get("dep4_gender")
        dep4_dob = request.form.get("dep4_dob")

        # Dependent 5
        dep5_name = request.form.get("dep5_name")
        dep5_relation = request.form.get("dep5_relation")
        dep5_gender = request.form.get("dep5_gender")
        dep5_dob = request.form.get("dep5_dob")

        # Save to database
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO gmc_forms (
                email, name, last_modified_time, employee_code,
                employee_name_aadhar, employee_gender, employee_dob_aadhar,
                dep1_name, dep1_relation, dep1_gender, dep1_dob,
                dep2_name, dep2_relation, dep2_gender, dep2_dob,
                dep3_name, dep3_relation, dep3_gender, dep3_dob,
                dep4_name, dep4_relation, dep4_gender, dep4_dob,
                dep5_name, dep5_relation, dep5_gender, dep5_dob
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            email, name, last_modified_time, employee_code,
            employee_name_aadhar, employee_gender, employee_dob_aadhar,
            dep1_name, dep1_relation, dep1_gender, dep1_dob,
            dep2_name, dep2_relation, dep2_gender, dep2_dob,
            dep3_name, dep3_relation, dep3_gender, dep3_dob,
            dep4_name, dep4_relation, dep4_gender, dep4_dob,
            dep5_name, dep5_relation, dep5_gender, dep5_dob
        ))
        conn.commit()
        conn.close()

        # Save to CSV as well
        csv_file = "gmc_data.csv"
        file_exists = os.path.isfile(csv_file)

        header = [
            "Email", "Name", "Last modified time", "Employee Code",
            "Employee Name (Aadhar)", "Employee Gender", "Employee DOB (Aadhar)",
            "Dep1 Name", "Dep1 Relation", "Dep1 Gender", "Dep1 DOB",
            "Dep2 Name", "Dep2 Relation", "Dep2 Gender", "Dep2 DOB",
            "Dep3 Name", "Dep3 Relation", "Dep3 Gender", "Dep3 DOB",
            "Dep4 Name", "Dep4 Relation", "Dep4 Gender", "Dep4 DOB",
            "Dep5 Name", "Dep5 Relation", "Dep5 Gender", "Dep5 DOB"
        ]

        row = [
            email, name, last_modified_time, employee_code,
            employee_name_aadhar, employee_gender, employee_dob_aadhar,
            dep1_name, dep1_relation, dep1_gender, dep1_dob,
            dep2_name, dep2_relation, dep2_gender, dep2_dob,
            dep3_name, dep3_relation, dep3_gender, dep3_dob,
            dep4_name, dep4_relation, dep4_gender, dep4_dob,
            dep5_name, dep5_relation, dep5_gender, dep5_dob
        ]

        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(header)
            writer.writerow(row)

        return "Form submitted successfully! Data saved in database and CSV."

    # GET request -> show the form
    return render_template("gmc_form.html")


# Upload CSV to populate gmc_master
@app.route("/upload-gmc", methods=["GET", "POST"])
def upload_gmc():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("Please upload a CSV file.")
            return redirect(request.url)

        try:
            stream = io.TextIOWrapper(file.stream, encoding="utf-8-sig")
            reader = csv.DictReader(stream)

            conn = get_db_connection()
            cur = conn.cursor()

            rows_to_insert = []

            for row in reader:
                employee_code   = (row.get("Employee Code") or "").strip()
                name            = (row.get("Name") or "").strip()
                official_dob    = (row.get("Official DOB") or "").strip()
                relation        = (row.get("Relation") or "").strip()
                gender          = (row.get("Gender") or "").strip()
                sum_assured     = (row.get("Sum Assured") or "").strip()
                age             = (row.get("Age") or "").strip()
                age_range       = (row.get("Range") or "").strip()
                total_premium   = parse_number(row.get("Total Premium"))
                monthly_premium = parse_number(row.get("/Months"))

                if not employee_code and not name:
                    continue

                rows_to_insert.append((
                    employee_code,
                    name,
                    official_dob,
                    relation,
                    gender,
                    sum_assured,
                    age,
                    age_range,
                    total_premium,
                    monthly_premium
                ))

            if rows_to_insert:
                cur.executemany("""
                    INSERT INTO gmc_master
                        (employee_code, name, official_dob, relation, gender,
                         sum_assured, age, "range", total_premium, monthly_premium)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows_to_insert)

            conn.commit()
            conn.close()

            flash(f"Uploaded {len(rows_to_insert)} records successfully.")
            return redirect(url_for("upload_gmc"))

        except Exception as e:
            flash("Error reading file: " + str(e))
            return redirect(request.url)

    return render_template("upload_gmc.html")


# Search / display GMC
@app.route("/search-gmc", methods=["GET", "POST"])
def search_gmc():
    search_emp = ""
    search_name = ""
    results = []
    summary = None
    suggestions = []

    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch suggestions (employee codes + self name)
    cur.execute("""
        SELECT
            employee_code,
            MAX(CASE WHEN LOWER(COALESCE(relation, '')) = 'self'
                     THEN name ELSE NULL END) AS employee_name
        FROM gmc_master
        WHERE employee_code IS NOT NULL
        GROUP BY employee_code
        ORDER BY employee_code
    """)
    suggestions = cur.fetchall()

    if request.method == "POST":
        search_emp = (request.form.get("employee_code") or "").strip()
        search_name = (request.form.get("name") or "").strip()

        query = "SELECT * FROM gmc_master WHERE 1=1"
        params = []

        if search_emp:
            query += " AND TRIM(employee_code) = ?"
            params.append(search_emp)
        elif search_name:
            query += " AND LOWER(TRIM(name)) LIKE ?"
            params.append(f"%{search_name.lower()}%")

        cur.execute(query, params)
        results = cur.fetchall()

        # ---------- Normalize rows and compute age + age_range for template ----------
        if results:
            results_list = []
            for r in results:
                try:
                    rd = dict(r)   # sqlite3.Row -> dict
                except Exception:
                    rd = r if isinstance(r, dict) else {}

                # canonical dob: prefer official_dob if present
                dob_val = rd.get('official_dob') or rd.get('dob') or rd.get('employee_dob') or rd.get('dep1_dob') or ""
                rd['dob'] = dob_val

                # numeric age: prefer stored 'age' else compute from dob
                age_val = None
                if rd.get('age') is not None and rd.get('age') != "":
                    try:
                        age_val = int(rd.get('age'))
                    except Exception:
                        age_val = None
                if age_val is None and dob_val:
                    age_val = calculate_age_from_dob(dob_val)
                rd['age'] = age_val if age_val is not None else ""

                # age_range: prefer existing DB column 'range', else compute from age
                if rd.get('age_range'):
                    rd['age_range'] = rd.get('age_range')
                elif rd.get('range'):
                    rd['age_range'] = rd.get('range')
                else:
                    rd['age_range'] = age_range_from_age(age_val)

                # ensure numeric premiums
                try:
                    rd['monthly_premium'] = float(rd.get('monthly_premium') or 0)
                except Exception:
                    rd['monthly_premium'] = 0.0
                try:
                    rd['total_premium'] = float(rd.get('total_premium') or 0)
                except Exception:
                    rd['total_premium'] = 0.0

                # other fields & fallbacks
                rd['sum_assured'] = rd.get('sum_assured') or rd.get('suminsured') or ""
                rd['name'] = rd.get('name') or rd.get('employee_name') or ""
                rd['relation'] = rd.get('relation') or ""
                rd['gender'] = rd.get('gender') or ""
                rd['employee_code'] = rd.get('employee_code') or rd.get('emp_code') or ""

                results_list.append(rd)

            # Build summary (prefer 'self' row for employee info)
            total_dependents = len(results_list)
            monthly_premium = sum(r.get('monthly_premium', 0) for r in results_list)
            annual_premium = sum(r.get('total_premium', 0) for r in results_list)
            self_row = next((r for r in results_list if (str(r.get('relation') or "").lower() == "self")), results_list[0])

            summary = {
                "employee_code": self_row.get('employee_code', ''),
                "employee_name": self_row.get('name', ''),
                "dependents": total_dependents,
                "monthly_premium": monthly_premium,
                "annual_premium": annual_premium
            }

            # overwrite results for template
            results = results_list
        else:
            results = []
            summary = None
        # -------------------------------------------------------------------------

    conn.close()

    return render_template(
        "search_gmc.html",
        search_emp=search_emp,
        search_name=search_name,
        results=results,
        summary=summary,
        suggestions=suggestions
    )


# ===== Helpdesk Report Routes =====

@app.route('/helpdesk_report')
def helpdesk_report():
    # last report ki range nikaalo, next start suggest karo
    last_start, last_end = get_last_report_range()

    suggested_start = None
    if last_end:
        try:
            d = datetime.strptime(last_end, "%Y-%m-%d").date()
            suggested_start = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception:
            suggested_start = None

    return render_template(
        'helpdesk_report.html',
        last_start=last_start,
        last_end=last_end,
        suggested_start=suggested_start
    )


@app.route('/helpdesk_upload', methods=['POST'])
def helpdesk_upload():
    global LAST_PIVOT_DF, LAST_DF2, LAST_META, LAST_RANGE, LAST_FILE_NAME

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})
    f = request.files['file']

    # remember file name
    LAST_FILE_NAME = f.filename or "ticket_report_input"

    try:
        df = load_file(f)
    except Exception as e:
        return jsonify({'error': f'Failed to read file: {e}'})

    id_col = find_col(df, {'ticket_id', 'ticketid', 'id'})
    created_col = find_col(df, {'created_at', 'created', 'createdat', 'created_date', 'created date'})
    updated_col = find_col(df, {'updated_at', 'updated', 'updatedat', 'updated_date', 'updated date'})
    status_col = find_col(df, {'status'})
    dept_col = find_col(df, {'department', 'dept'})

    missing = []
    for name, val in [('ticket_id', id_col), ('created_at', created_col), ('updated_at', updated_col),
                      ('Status', status_col), ('Department', dept_col)]:
        if val is None:
            missing.append(name)
    if missing:
        return jsonify({'error': f'Missing required columns: {", ".join(missing)}'})

    df2 = compute_buckets(df, created_col, updated_col)

    LAST_DF2 = df2
    LAST_META = {
        'id_col': id_col,
        'status_col': status_col,
        'dept_col': dept_col
    }

    payload = build_payload_from_df(df2, id_col, status_col, dept_col)
    LAST_PIVOT_DF = build_excel_df(payload)

    # upload ke baad abhi koi filter nahi laga, range reset
    LAST_RANGE = None

    return jsonify(payload)


@app.route('/helpdesk_filter', methods=['POST'])
def helpdesk_filter():
    global LAST_DF2, LAST_PIVOT_DF, LAST_META, LAST_RANGE
    if LAST_DF2 is None or not LAST_META:
        return jsonify({'error': 'No data loaded yet. Please upload a file first.'})

    req = request.get_json(force=True, silent=True) or {}
    start_s = (req.get('start_date') or '').strip()
    end_s = (req.get('end_date') or '').strip()

    df2 = LAST_DF2

    mask = pd.Series(True, index=df2.index)

    if start_s:
        sdt = safe_parse_dt(start_s)
        if sdt:
            sdate = sdt.date()
            mask &= df2['_cdate'].apply(lambda d: (d is not None) and (d >= sdate))

    if end_s:
        edt = safe_parse_dt(end_s)
        if edt:
            edate = edt.date()
            mask &= df2['_cdate'].apply(lambda d: (d is not None) and (d <= edate))

    filtered = df2[mask].copy()

    if filtered.empty:
        return jsonify({'error': 'No records found for selected date range.'})

    id_col = LAST_META['id_col']
    status_col = LAST_META['status_col']
    dept_col = LAST_META['dept_col']

    payload = build_payload_from_df(filtered, id_col, status_col, dept_col)
    LAST_PIVOT_DF = build_excel_df(payload)

    # current filter ko active range mana jayega
    def norm(dstr):
        if not dstr:
            return None
        dt = safe_parse_dt(dstr)
        if not dt:
            return None
        return dt.date().strftime("%Y-%m-%d")

    LAST_RANGE = {
        'start': norm(start_s),
        'end': norm(end_s)
    }

    return jsonify(payload)


@app.route('/helpdesk_download')
def helpdesk_download():
    global LAST_PIVOT_DF
    if LAST_PIVOT_DF is None:
        return 'No report ready', 400

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        LAST_PIVOT_DF.to_excel(writer, index=False, sheet_name='Ticket Report')
    buf.seek(0)

    return send_file(
        buf,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='ticket_report.xlsx'
    )


@app.route('/helpdesk_mark_sent', methods=['POST'])
def helpdesk_mark_sent():
    global LAST_RANGE, LAST_FILE_NAME

    if LAST_RANGE is None or LAST_RANGE.get('start') is None:
        return jsonify({'error': 'Please apply a date filter before marking as sent.'})

    start_date = LAST_RANGE['start']
    end_date = LAST_RANGE.get('end')
    file_name = LAST_FILE_NAME or "ticket_report_input"

    save_report_history(start_date, end_date, file_name)

    return jsonify({'message': f'Report range {start_date} to {end_date or "..." } marked as sent.'})


@app.route('/helpdesk_send_email', methods=['POST'])
def helpdesk_send_email():
    global LAST_PIVOT_DF, LAST_RANGE

    if LAST_PIVOT_DF is None:
        return jsonify({'error': 'No report is ready. Please upload and filter first.'})

    start_date = LAST_RANGE['start'] if LAST_RANGE else None
    end_date = LAST_RANGE['end'] if LAST_RANGE else None

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        LAST_PIVOT_DF.to_excel(writer, index=False, sheet_name='Ticket Report')
    buf.seek(0)
    excel_bytes = buf.read()

    try:
        send_report_email(excel_bytes, start_date, end_date)
    except Exception as e:
        return jsonify({'error': f'Failed to send email: {e}'})

    return jsonify({'message': 'Email sent successfully to manager.'})


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
