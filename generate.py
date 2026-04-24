import os, re, requests, urllib3
from datetime import date
from collections import defaultdict

urllib3.disable_warnings()

DATABRICKS_HOST = "https://krafton-hq.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = "98481282e9f1b19c"

def run_query(sql):
    r = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"statement": sql, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "120s"},
        verify=False
    )
    r.raise_for_status()
    d = r.json()
    if d.get("status", {}).get("state") == "FAILED":
        raise RuntimeError(d["status"]["error"]["message"])
    cols = [c["name"] for c in d["manifest"]["schema"]["columns"]]
    rows = d["result"].get("data_array", [])
    return [dict(zip(cols, row)) for row in rows]

KPI_SQL = """
WITH base AS (
  SELECT
    CASE WHEN YEAR(std_dt) < 2024 THEN DATE_FORMAT(std_dt,'yyyy-MM')
         ELSE DATE_FORMAT(ADD_MONTHS(std_dt,-1),'yyyy-MM') END AS month,
    pay_amt_tag AS lv,
    BU, NBU, Repay, Churn_Pay,
    Revenue, NBU_Revenue, Repay_Revenue, Return_Pay_Revenue,
    ARPPU, NBU_Revenue_Rate, Repay_Revenue_Rate, ReturnPay_Revenue_Rate, NBU_Rate,
    ROW_NUMBER() OVER (
      PARTITION BY country,
        CASE WHEN YEAR(std_dt)<2024 THEN DATE_FORMAT(std_dt,'yyyy-MM')
             ELSE DATE_FORMAT(ADD_MONTHS(std_dt,-1),'yyyy-MM') END,
      pay_amt_tag ORDER BY std_dt DESC
    ) AS rn
  FROM pubgm_mart.monthly_saleslevel_stat
  WHERE country = 'JP'
    AND pay_amt_tag IN ('Lv_1','Lv_2','Lv_3','Lv_4','Lv_5','Lv_6','Lv_7','Non-Paid')
)
SELECT month, lv, BU, NBU, Repay, Churn_Pay,
       Revenue, NBU_Revenue, Repay_Revenue, Return_Pay_Revenue,
       ARPPU, NBU_Revenue_Rate, Repay_Revenue_Rate, ReturnPay_Revenue_Rate, NBU_Rate
FROM base WHERE rn = 1
ORDER BY month, lv
"""

UC_SQL = """
SELECT DATE_FORMAT(std_dt,'yyyy-MM') AS month,
       pay_amt_duringLast30days_tag AS lv,
       SUM(UC_usage) AS uc
FROM pubgm_bi.bu_uc_usage_info_monthly
WHERE country = 'JP'
  AND pay_amt_duringLast30days_tag IN ('Lv_1','Lv_2','Lv_3','Lv_4','Lv_5','Lv_6','Lv_7')
GROUP BY month, lv
ORDER BY month, lv
"""

PAID_LVS = ['Lv_1','Lv_2','Lv_3','Lv_4','Lv_5','Lv_6','Lv_7']

def v(val):
    if val is None: return 'null'
    try:
        f = float(val)
        return str(int(f)) if f == int(f) else str(round(f, 2))
    except:
        return 'null'

print("Querying KPI data...")
kpi_rows = run_query(KPI_SQL)
print(f"  {len(kpi_rows)} rows")

# Build RAW array
raw_lines = []
months_seen = set()
for r in kpi_rows:
    m, lv = r['month'], r['lv']
    months_seen.add(m)
    if lv == 'Non-Paid':
        line = f'["{m}","Non-Paid",0,0,0,{v(r["Churn_Pay"])},0,0,0,0,null,null,null,null,null]'
    else:
        line = (f'["{m}","{lv}",'
                f'{v(r["BU"])},{v(r["NBU"])},{v(r["Repay"])},0,'
                f'{v(r["Revenue"])},{v(r["NBU_Revenue"])},{v(r["Repay_Revenue"])},'
                f'{v(r["Return_Pay_Revenue"])},{v(r["ARPPU"])},'
                f'{v(r["NBU_Revenue_Rate"])},{v(r["Repay_Revenue_Rate"])},'
                f'{v(r["ReturnPay_Revenue_Rate"])},{v(r["NBU_Rate"])}]')
    raw_lines.append(line)

raw_js = 'const RAW = [\n' + ',\n'.join(raw_lines) + '\n];'

print("Querying UC data...")
uc_rows = run_query(UC_SQL)
print(f"  {len(uc_rows)} rows")

# Build UC_USAGE object
uc_by_month = defaultdict(dict)
for r in uc_rows:
    uc_by_month[r['month']][r['lv']] = int(float(r['uc']))

uc_lines = []
for month in sorted(uc_by_month.keys()):
    vals = [str(uc_by_month[month].get(lv, 0)) for lv in PAID_LVS]
    uc_lines.append(f'"{month}":[{",".join(vals)}]')

uc_js = ('// UC_USAGE: index 0=Lv_1, 1=Lv_2, 2=Lv_3, 3=Lv_4, 4=Lv_5, 5=Lv_6, 6=Lv_7\n'
         'const UC_USAGE = {\n' + ',\n'.join(uc_lines) + '\n};')

# Generate HTML
with open('template.html', 'r', encoding='utf-8') as f:
    html = f.read()

today = date.today().isoformat()
html = html.replace('// __RAW_DATA__', raw_js)
html = html.replace('// __UC_DATA__', uc_js)
html = re.sub(r'추출일: \d{4}-\d{2}-\d{2}', f'추출일: {today}', html)

# Update warning badges based on missing months
all_months = sorted(months_seen)
latest = all_months[-1] if all_months else ''
html = re.sub(r'<span class="badge badge-warn">.*?누락</span>', '', html)
html = re.sub(r'<span class="badge badge-warn">.*?미집계</span>', '', html)

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Done! index.html generated (latest KPI month: {latest})")
