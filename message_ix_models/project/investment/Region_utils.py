# ==============================
# 📦 Region and country mapping utilities
# ==============================

import pycountry

# Manual override for name-to-ISO code conversion
manual_map = {
    "Korea, Dem. People's Rep.": "PRK",
    "Korea, Rep.": "KOR",
    "Egypt, Arab Rep.": "EGY",
    "Iran, Islamic Rep.": "IRN",
    "Venezuela, RB": "VEN",
    "Russian Federation": "RUS",
    "Vietnam": "VNM",
    "Czechia": "CZE",
    "Slovak Republic": "SVK",
    "Hong Kong SAR, China": "HKG",
    "Gambia, The": "GMB"
}

def get_country_code(name):
    """
    Convert country name to ISO Alpha-3 code (with manual fallback).
    """
    if name in manual_map:
        return manual_map[name]
    try:
        return pycountry.countries.lookup(name).alpha_3
    except LookupError:
        return None

def get_country_name(name):
    """
    Convert country name or code to canonical country name.
    """
    if name in manual_map:
        return pycountry.countries.get(alpha_3=manual_map[name]).name
    try:
        return pycountry.countries.lookup(name).name
    except LookupError:
        return name

def get_region_mapping():
    """
    Return mapping of R12 regions to lists of ISO Alpha-3 country codes.
    """
    return {
        "R12_AFR": ["AGO", "BDI", "BEN", "BFA", "BWA", "CAF", "CIV", "CMR", "COD", "COG", "COM", "CPV", "DJI", "ERI", "ETH", "GAB", "GHA", "GIN", "GMB", "GNB", "GNQ", "KEN", "LBR", "LSO", "MDG", "MLI", "MOZ", "MRT", "MUS", "MWI", "MYT", "NAM", "NER", "NGA", "REU", "RWA", "SEN", "SHN", "SLE", "SOM", "STP", "SWZ", "SYC", "TCD", "TGO", "TZA", "UGA", "ZAF", "ZMB", "ZWE"],
        "R12_RCPA": ["KHM", "LAO", "MNG", "PRK", "VNM"],
        "R12_CHN": ["CHN", "HKG"],
        "R12_EEU": ["ALB", "BGR", "BIH", "CZE", "EST", "HRV", "HUN", "LTU", "LVA", "MKD", "MNE", "POL", "ROU", "SCG", "SRB", "SVK", "SVN", "YUG"],
        "R12_FSU": ["ARM", "AZE", "BLR", "GEO", "KAZ", "KGZ", "MDA", "RUS", "TJK", "TKM", "UKR", "UZB"],
        "R12_LAM": ["ABW", "AIA", "ANT", "ARG", "ATG", "BES", "BHS", "BLZ", "BMU", "BOL", "BRA", "BRB", "CHL", "COL", "CRI", "CUB", "CUW", "CYM", "DMA", "DOM", "ECU", "FLK", "GLP", "GRD", "GTM", "GUF", "GUY", "HND", "HTI", "JAM", "KNA", "LCA", "MEX", "MSR", "MTQ", "NIC", "PAN", "PER", "PRY", "SLV", "SUR", "SXM", "TCA", "TTO", "URY", "VCT", "VEN", "VGB"],
        "R12_MEA": ["ARE", "BHR", "DZA", "EGY", "ESH", "IRN", "IRQ", "ISR", "JOR", "KWT", "LBN", "LBY", "MAR", "OMN", "PSE", "QAT", "SAU", "SDN", "SSD", "SYR", "TUN", "YEM"],
        "R12_NAM": ["CAN", "GUM", "PRI", "SPM", "USA", "VIR"],
        "R12_PAO": ["AUS", "JPN", "NZL"],
        "R12_PAS": ["ASM", "BRN", "CCK", "COK", "CXR", "FJI", "FSM", "IDN", "KIR", "KOR", "MAC", "MHL", "MMR", "MNP", "MYS", "NCL", "NFK", "NIU", "NRU", "PCI", "PCN", "PHL", "PLW", "PNG", "PYF", "SGP", "SLB", "THA", "TKL", "TLS", "TON", "TUV", "TWN", "VUT", "WLF", "WSM"],
        "R12_SAS": ["AFG", "BGD", "BTN", "IND", "LKA", "MDV", "NPL", "PAK"],
        "R12_WEU": ["AND", "AUT", "BEL", "CHE", "CYP", "DEU", "DNK", "ESP", "FIN", "FRA", "FRO", "GBR", "GIB", "GRC", "GRL", "IMN", "IRL", "ISL", "ITA", "LIE", "LUX", "MCO", "MLT", "NLD", "NOR", "PRT", "SJM", "SMR", "SWE", "TUR", "VAT"]
    }

def get_region(code):
    """
    Get R12 region name for a given ISO Alpha-3 country code.
    """
    region_mapping = get_region_mapping()
    for region, codes in region_mapping.items():
        if code in codes:
            return region
    return None
