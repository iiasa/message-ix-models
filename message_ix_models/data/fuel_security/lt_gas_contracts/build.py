"""Merge DIW (1965-2014), curated (2015-2024 + pipelines) and GFB (2025-) contracts."""
import csv
import sys

import openpyxl

sys.path.insert(0, sys.argv[4])
from curated import LNG, PIPE, PORT  # noqa: E402

DIW_CSV, GFB_XLSX, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
BCM_PER_MT = 1.36
MIN_END = 2025
DIW_URL = "https://d-nb.info/1152140868/34"  # Neumann, Ruester & von Hirschhausen (2015), DIW Data Doc. 77
GFB_URL = "https://gfbinsight.com/lng-data-hub/"

ISO = {
    "Algeria": "DZA", "Argentina": "ARG", "Australia": "AUS", "Austria": "AUT", "Azerbaijan": "AZE",
    "Bangladesh": "BGD", "Belgium": "BEL", "Bosnia and Herzegovina": "BIH", "Albania": "ALB",
    "Brazil": "BRA", "Brunei": "BRN", "Bulgaria": "BGR", "Canada": "CAN", "Chile": "CHL",
    "China": "CHN", "Czech Republic": "CZE", "Denmark": "DNK", "Dominican Republic": "DOM",
    "Egypt": "EGY", "France": "FRA", "Germany": "DEU", "Greece": "GRC", "Hungary": "HUN",
    "India": "IND", "Indonesia": "IDN", "Iran": "IRN", "Iraq": "IRQ", "Israel": "ISR",
    "Italy": "ITA", "Japan": "JPN", "Jordan": "JOR", "Kuwait": "KWT", "Libya": "LBY",
    "Lithuania": "LTU", "Malaysia": "MYS", "Mexico": "MEX", "Mozambique": "MOZ", "Myanmar": "MMR",
    "Netherlands": "NLD", "Nigeria": "NGA", "Norway": "NOR", "Oman": "OMN", "Pakistan": "PAK",
    "Papua New Guinea": "PNG", "Philippines": "PHL", "Poland": "POL", "Portugal": "PRT",
    "Puerto Rico": "PRI", "Qatar": "QAT", "Romania": "ROU", "Russia": "RUS", "Saudi Arabia": "SAU",
    "Serbia": "SRB", "Singapore": "SGP", "South Africa": "ZAF", "South Korea": "KOR", "Spain": "ESP",
    "Switzerland": "CHE", "Taiwan": "TWN", "Thailand": "THA", "Trinidad and Tobago": "TTO",
    "Turkey": "TUR", "Turkmenistan": "TKM", "Ukraine": "UKR", "United Arab Emirates": "ARE",
    "United Kingdom": "GBR", "United States": "USA", "Vietnam": "VNM", "Yemen": "YEM",
    "Senegal/Mauritania": "SEN/MRT", "France/Spain": "FRA/ESP", PORT: PORT,
}

rows = []


def add(exp, imp, mode, start, end, amount, unit, buyer, seller, agr, status, url, dataset, verif, note):
    bcm = amount * BCM_PER_MT if (amount is not None and unit == "mtpa") else amount
    mt = amount if unit == "mtpa" else (amount / BCM_PER_MT if amount is not None else None)
    rows.append({
        "Exporting Country": exp, "Importing Country": imp, "Pipeline or LNG": mode,
        "Contract Start Year": start, "Contract End Year": end,
        "Trade Amount Per Year": amount, "Unit": unit,
        "Trade Amount (bcm/yr)": round(bcm, 3) if bcm is not None else None,
        "Trade Amount (Mt LNG-eq/yr)": round(mt, 3) if mt is not None else None,
        "Exporter ISO3": ISO.get(exp, ""), "Importer ISO3": ISO.get(imp, ""),
        "Buyer": buyer, "Seller": seller, "Agreement Type": agr, "Status": status,
        "Announcement or Source URL": url, "Source Dataset": dataset,
        "Verification": verif, "Notes": note,
    })


def status_for(start, end, agr, base=None):
    if base:
        return base
    if agr in ("HOA", "MOU"):
        return "non-binding"
    if start is None:
        return "contracted, start TBD"
    return "in force" if start <= 2026 else "contracted, future start"


# ---------------------------------------------------------------- DIW
RU_EU = {"Austria", "Czech", "Czech Republic", "Denmark", "Germany", "Italy", "Romania", "Poland",
         "France", "Netherlands", "Finland", "Greece", "Slovenia", "Switzerland", "Belgium", "United Kingdom"}
for r in csv.DictReader(open(DIW_CSV)):
    if r["end"].startswith("n.a"):
        continue
    end = int(float(r["end"]))
    if end < MIN_END:
        continue
    start = int(float(r["start"])) if not r["start"].startswith("n.a") else None
    exp, imp, mode = r["seller_country"], r["buyer_country"], r["product"]
    imp = {"Czech": "Czech Republic", "??": PORT}.get(imp, imp)
    note, status, url = "DIW original source: " + r["source"], None, DIW_URL
    if r["id"] == "2":  # Endesa is Spanish
        imp = "Spain"; note += "; buyer country corrected Germany->Spain"
    if r["id"] == "106":  # GAIL-Gazprom Singapore is an LNG portfolio deal
        mode, exp = "LNG", PORT; status = "terminated/defaulted"
        note += "; LNG portfolio deal (not pipeline); Gazprom deliveries defaulted 2022"
    elif exp == "Russia" and mode == "Pipeline" and imp in RU_EU:
        status = "terminated/suspended"
        note += "; Russian pipeline supply to EU halted/terminated 2022-2025; EU ban on Russian gas contracts by end-2027"
    elif exp == "Russia" and imp == "Turkey":
        status = "superseded"; note += "; see curated Blue Stream row"
    elif exp == "Turkmenistan" and imp == "Russia":
        status = "intra-FSU / uncertain"
    elif exp == "Qatar" and imp == "United Arab Emirates":
        status = "superseded"; note += "; see curated Dolphin row"
    elif exp == "Yemen":
        status = "force majeure"; note += "; Yemen LNG offline since 2015"
    elif exp == "Iran":
        status = "never implemented"
    elif exp == "Russia" and mode == "LNG" and imp != "PORTFOLIO":
        note += "; Sakhalin-2 transferred to Russian operator 2022; Japanese contracts continued"
    elif exp == "Turkmenistan" and imp == "China":
        note += "; plus additional 25 bcm (2011) for Line D, not built"
    if start is not None and status is None:
        status = "in force" if start <= 2026 else "contracted, future start"
    add(exp, imp, mode, start, end, float(r["bcm_yr"]), "bcm/yr", r["buyer_co"], r["seller_co"],
        "SPA", status, url, "DIW DD77 (2015)", "published dataset", note)

# ---------------------------------------------------------------- curated
for exp, imp, s, e, v, b, sel, agr, url, note, ver in LNG:
    add(exp, imp, "LNG", s, e, v, "mtpa", b, sel, agr, status_for(s, e, agr), url, "Curated (this compilation)",
        "source-checked" if ver == "url" else "compiler knowledge - verify", note)
for exp, imp, s, e, v, b, sel, agr, url, note, ver in PIPE:
    if exp == "Turkmenistan" and imp == "China":
        continue  # DIW row kept
    st = "expiring" if e <= 2026 else status_for(s, e, agr)
    add(exp, imp, "Pipeline", s, e, v, "bcm/yr", b, sel, agr, st, url, "Curated (this compilation)",
        "source-checked" if ver == "url" else "compiler knowledge - verify", note)

# ---------------------------------------------------------------- GFB
BUY = {  # buyer company -> importing country (PORTFOLIO = aggregator / destination-flexible)
    "EnBW": "Germany", "Petrobangla": "Bangladesh", "IOC": "India", "GSPC": "India", "BPCL": "India",
    "Petrobras": "Brazil", "Osaka Gas": "Japan", "LNGPH": "Philippines", "China Rsources": "China",
    "CPC": "Taiwan", "SEFE": "Germany", "GAIL": "India", "Aramco": "Saudi Arabia",
    "ENADOM": "Dominican Republic", "Uniper": "Germany", "Mitsui": "Japan", "Mistui": "Japan",
    "ENN": "China", "CNOOC": "China", "Zhenhua Oil": "China", "Covestro": "Germany",
    "PetroChina": "China", "Guangzhou Gas": "China", "Kyuhsu Electric": "Japan",
    "Kyushu Electric": "Japan", "JERA": "Japan", "Zhejiang Energy": "China",
    "Guangdong Pearl River": "China", "Torrent Power": "India", "PTT": "Thailand",
    "Centrica": "United Kingdom", "A2A": "Italy", "HPCL": "India", "KOGAS": "South Korea",
    "MVM": "Hungary", "Edison": "Italy", "Keppel": "Singapore", "POSCO": "South Korea",
    "Puerto Rico Government": "Puerto Rico", "BOTAS": "Turkey", "Hyundai Chemical": "South Korea",
    "Tokyo Gas": "Japan", "Naturgy": "Spain", "Atlantic - SEE LNG": "Greece", "Gulf": "Thailand",
    "Gulf Development": "Thailand", "Shizuoka Gas": "Japan", "Hokkaido Gas": "Japan",
    "Hanwha": "South Korea", "Metlen": "Greece", "INEOS": "United Kingdom", "Albgaz": "Albania",
    "Aluminij Industries": "Bosnia and Herzegovina", "PGN": "Indonesia", "PLN": "Indonesia",
    "Zululand Energy Terminal": "South Africa", "Petrovietnam Gas": "Vietnam", "PV Gas": "Vietnam",
    "Japex": "Japan", "Naftogaz": "Ukraine", "Ignitis": "Lithuania",
    "China Suntien Green Energy": "China", "China Gas": "China", "Hokuriku Electric": "Japan",
    "INPEX": "Japan", "RWE": "Germany", "Petronas": PORT, "OQ Trading": PORT,
}
SELL = {  # seller -> exporting country
    "ADNOC": "United Arab Emirates", "Argent LNG": "United States", "Oman LNG": "Oman",
    "Alaska LNG": "United States", "Delfin LNG": "United States", "QatarEnergy": "Qatar",
    "Next Decade": "United States", "Energy Transfer": "United States", "Amigo LNG": "Mexico",
    "Brunei LNG": "Brunei", "Commonwealth LNG": "United States", "Sempra": "United States",
    "Cheniere": "United States", "Venture Global": "United States", "Texas LNG": "United States",
    "Monkey Island LNG": "United States", "Cedar LNG": "Canada", "Southern Energy": "Argentina",
    "Petronas": "Malaysia", "LNG del Plata": "Argentina", "Abadi LNG": "Indonesia",
    "North Hub and South Hub Gas Projects, Indonesia": "Indonesia",
    "North Hub Gas Project, Indonesia": "Indonesia", "Ksi Lisims": "Canada",
    "Ksi Lisims LNG": "Canada", "Papua LNG": "Papua New Guinea", "Kino Aski LNG": "Canada",
    "Santos": "Australia", "EQT": "United States", "Brunei": "Brunei", "ARC Resources": "Canada",
}
wb = openpyxl.load_workbook(GFB_XLSX, read_only=True, data_only=True)
for r in list(wb["Long-term LNG contracts"].iter_rows(values_only=True))[1:]:
    if r[0] is None:
        continue
    _, date, buyer, seller, q, dur, start, agr, ptype = r[:9]
    imp = BUY.get(buyer, PORT)
    exp = SELL.get(seller, PORT)
    note = f"GFB #{r[0]}; reported {date:%Y-%m-%d}; project type at signing: {ptype}"
    if seller == "Woodside" and ptype in ("Pre-FID", "Under construction"):
        exp = "United States"; note += "; assumed Woodside Louisiana LNG"
    if seller == "ARC Resources":
        note += "; gas-supply-for-LNG netback deal"
    end = start + dur - 1 if (start and dur) else None
    st = status_for(start, end, agr)
    if agr == "SPA" and ptype == "Pre-FID":
        st = "contracted, project pre-FID"
    if exp == "Qatar":
        note += "; QatarEnergy force majeure since Mar 2026 (Iran attacks)"
    add(exp, imp, "LNG", start, end, q, "mtpa", buyer, seller, agr, st, GFB_URL,
        "GFB Insight LT LNG contracts DB (2026-11-02)", "published dataset", note)

# Qatar force-majeure flag on all Qatar-origin LNG rows
for row in rows:
    if row["Exporting Country"] == "Qatar" and row["Pipeline or LNG"] == "LNG" and "force majeure" not in row["Notes"]:
        row["Notes"] += "; QatarEnergy force majeure on deliveries since Mar 2026 (Iran attacks; ~17% capacity offline)"

cols = list(rows[0].keys())
with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows(sorted(rows, key=lambda x: (x["Pipeline or LNG"], x["Exporting Country"], x["Importing Country"], x["Contract Start Year"] or 9999)))
print(len(rows), "rows ->", OUT)
