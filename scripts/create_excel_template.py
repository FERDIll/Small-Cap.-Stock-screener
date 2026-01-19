from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import FormulaRule, CellIsRule

out = Path("data/defense_screening_prototype_v1.xlsx")
out.parent.mkdir(parents=True, exist_ok=True)

HEADERS = [
    # Quick feel
    "Ticker","Company Name","Status","SIC Description",
    "P_Market_Cap_USD","A_TTM_Revenue_USD","A_Revenue_YoY_Growth","A_Operating_Margin","P_ADV30_Dollar",
    "P_Close_Price_USD","P_Price_Date",

    # Deeper
    "A_Current_Ratio","A_Debt_to_Equity","A_RnD_Intensity","A_TTM_OCF_USD","A_TTM_FCF_USD",

    # Deep analytics (collapse)
    "A_Cash_USD","A_Total_Debt_USD","A_Shares_Outstanding","A_Shares_AsOf",
    "A_SGandA_Intensity","A_SBC_USD_Latest","A_TTM_Capex_USD",

    # Gates + provenance + clutter (collapse)
    "G1_Pass_Reporting","G1_Reason","G1_Latest_10K","G1_Latest_10Q","G1_Latest_20F",
    "G2_Pass_Basics","G2_Reason","G2_Size_OK","G2_Shares_OK","G2_Operating_Exists","G2_CashOrOCF_OK",
    "G3_Pass","G3_Reason","P_MarketCap_Method","P_ADV30_Shares",
    "CIK","SIC","Data As-Of Date","Run Timestamp (UTC)",

    # These may appear if tiers/flags are enabled; keep them far right but present
    "C_10Q_Count_365d","C_10K_Count_365d","C_8K_Count_365d","C_S1_or_S3_Count_365d","C_424B_Count_365d",
    "C_13D_13G_Count_365d","C_Form4_Count_90d","C_Latest_10Q_Date","C_Latest_10K_Date","C_Latest_Shelf_Date",
    "C_Latest_Form4_Date","C_Recent_Filings_Listed",
    "B_Form4_Net_Shares_180d","B_Form4_Tx_Count_180d","B_Form4_Last_Date_Parsed",
    "F_TTM_Revenue_lt_500m","F_YoY_Abs_gt_40pct","F_OpMargin_lt_neg10pct","F_RnD_gt_20pct","F_SGandA_gt_50pct",
    "F_CurrentRatio_lt_1_5","F_DebtEq_gt_1_0",
]

COL = {h: i+1 for i, h in enumerate(HEADERS)}

wb = Workbook()
ws = wb.active
ws.title = "Universe"
ws.append(HEADERS)

# Header style
ws.row_dimensions[1].height = 28
fill = PatternFill("solid", fgColor="1F2937")
font = Font(bold=True, color="FFFFFF")
align = Alignment(horizontal="center", vertical="center", wrap_text=True)
for c in range(1, len(HEADERS)+1):
    cell = ws.cell(row=1, column=c)
    cell.fill = fill
    cell.font = font
    cell.alignment = align

# Freeze + filter
ws.freeze_panes = "E2"
ws.auto_filter.ref = f"A1:{get_column_letter(len(HEADERS))}1"

# Column widths (basic)
def set_w(name, w):
    if name in COL:
        ws.column_dimensions[get_column_letter(COL[name])].width = w

set_w("Ticker", 10)
set_w("Company Name", 32)
set_w("Status", 18)
set_w("SIC Description", 28)
for n in ["P_Market_Cap_USD","A_TTM_Revenue_USD","P_ADV30_Dollar","A_TTM_OCF_USD","A_TTM_FCF_USD",
          "A_Cash_USD","A_Total_Debt_USD","A_Shares_Outstanding","A_TTM_Capex_USD","P_Close_Price_USD"]:
    set_w(n, 16)
for n in ["A_Revenue_YoY_Growth","A_Operating_Margin","A_Current_Ratio","A_Debt_to_Equity","A_RnD_Intensity","A_SGandA_Intensity"]:
    set_w(n, 14)
for n in ["G1_Reason","G2_Reason","G3_Reason"]:
    set_w(n, 34)

# Collapse deep + clutter columns by default
deep_start = COL["A_Cash_USD"]
deep_end = COL["A_TTM_Capex_USD"]
clutter_start = COL["G1_Pass_Reporting"]
clutter_end = COL["F_DebtEq_gt_1_0"]
ws.column_dimensions.group(get_column_letter(deep_start), get_column_letter(deep_end), hidden=True)
ws.column_dimensions.group(get_column_letter(clutter_start), get_column_letter(clutter_end), hidden=True)
ws.sheet_properties.outlinePr.summaryBelow = False

# Conditional formatting (binary)
GREEN = PatternFill("solid", fgColor="C6EFCE")
RED = PatternFill("solid", fgColor="FFC7CE")
ORANGE = PatternFill("solid", fgColor="FFEB9C")
max_row = 5000

def rng(name):
    col = get_column_letter(COL[name])
    return f"{col}2:{col}{max_row}"

if "Status" in COL:
    c = get_column_letter(COL["Status"])
    ws.conditional_formatting.add(rng("Status"), FormulaRule(formula=[f'ISNUMBER(SEARCH("OK",${c}2))'], fill=GREEN))
    ws.conditional_formatting.add(rng("Status"), FormulaRule(formula=[f'ISNUMBER(SEARCH("Excluded",${c}2))'], fill=RED))

if "G3_Pass" in COL:
    c = get_column_letter(COL["G3_Pass"])
    ws.conditional_formatting.add(rng("G3_Pass"), FormulaRule(formula=[f'${c}2="TRUE"'], fill=GREEN))
    ws.conditional_formatting.add(rng("G3_Pass"), FormulaRule(formula=[f'${c}2="FALSE"'], fill=RED))

if "P_Market_Cap_USD" in COL:
    ws.conditional_formatting.add(rng("P_Market_Cap_USD"), CellIsRule(operator="lessThan", formula=["1000000000"], fill=GREEN))
    ws.conditional_formatting.add(rng("P_Market_Cap_USD"), CellIsRule(operator="greaterThan", formula=["5000000000"], fill=ORANGE))

if "A_Revenue_YoY_Growth" in COL:
    ws.conditional_formatting.add(rng("A_Revenue_YoY_Growth"), CellIsRule(operator="greaterThan", formula=["0.30"], fill=GREEN))
    ws.conditional_formatting.add(rng("A_Revenue_YoY_Growth"), CellIsRule(operator="lessThan", formula=["0"], fill=RED))

if "A_Operating_Margin" in COL:
    ws.conditional_formatting.add(rng("A_Operating_Margin"), CellIsRule(operator="greaterThan", formula=["0.10"], fill=GREEN))
    ws.conditional_formatting.add(rng("A_Operating_Margin"), CellIsRule(operator="lessThan", formula=["0"], fill=RED))

if "P_ADV30_Dollar" in COL:
    ws.conditional_formatting.add(rng("P_ADV30_Dollar"), CellIsRule(operator="lessThan", formula=["2000000"], fill=RED))
    ws.conditional_formatting.add(rng("P_ADV30_Dollar"), CellIsRule(operator="greaterThan", formula=["10000000"], fill=GREEN))

wb.save(out)
print("Created template:", out.resolve())
