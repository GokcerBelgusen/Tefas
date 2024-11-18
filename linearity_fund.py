import sqlite3
import pandas as pd
from scipy.stats import linregress
from datetime import datetime

# Database path
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# Array of funds to analyze

fund_codes = [
"AAK","AAL","AAS","AAV","AC1","AC4","AC5","AC6","ACC","ACD","ACK",
"ACU","ADE","ADP","AED","AES","AEV","AFA","AFO","AFS","AFT","AFV",
"AGC","AHI","AHN","AHU","AHV","AIS","AJK","AK2","AK3","AKE","AKU",
"ALC","AN1","ANZ","AOJ","AOY","APJ","APT","ARE","ARL","ARM","AS1",
"ASJ","AUT","AUV","AYA","AYR","BBF","BBP","BDC","BDS","BDY","BFE",
"BGP","BHF","BHI","BHL","BID","BIH","BIO","BNC","BST","BTE","BTZ",
"BUL","BUY","BVM","BVV","CFO","CIN","CKS","CPT","CPU","CVK","DAH",
"DAS","DBA","DBB","DBH","DBK","DBP","DBZ","DCB","DDA","DEF","DFC",
"DFD","DFI","DGH","DHJ","DHM","DKH","DKR","DL2","DLD","DLY","DMG",
"DNM","DOL","DOV","DPB","DPK","DPT","DSD","DSP","DTL","DTZ","DVT",
"DXP","DYN","DZE","DZM","EBD","EC2","ECA","EDT","EDU","EIB","EIC",
"EID","EIL","EKF","ELZ","ENJ","ESG","ESP","EUN","EUZ","EYT","FBC",
"FBI","FBV","FBZ","FCK","FD1","FDG","FFH","FFP","FI3","FIB","FID",
"FIL","FIT","FJB","FJZ","FKE","FLY","FMG","FMR","FNO","FNT","FPE",
"FPH","FPI","FPK","FPZ","FS5","FS6","FSF","FSG","FSH","FSK","FSR",
"FUA","FUB","FYD","FYO","FZJ","FZP","GA1","GAE","GAF","GAG","GAH",
"GAK","GAS","GBC","GBG","GBH","GBJ","GBL","GBN","GBV","GBZ","GGK",
"GHS","GID","GIE","GIH","GJB","GKF","GKH","GKS","GKV","GL1","GLC",
"GLG","GLS","GMA","GMC","GMD","GMR","GO1","GO2","GO3","GO4","GO6",
"GO9","GOH","GOL","GPA","GPB","GPC","GPF","GPG","GPI","GPL","GPT",
"GPU","GRL","GRO","GSM","GSP","GTA","GTF","GTM","GTY","GTZ","GUB",
"GUH","GUM","GUV","GVA","GVI","GYK","GZE","GZG","GZH","GZJ","GZL",
"GZM","GZN","GZP","GZR","GZV","GZY","GZZ","HAM","HAT","HBF","HBN",
"HBU","HDA","HDK","HEH","HGM","HGV","HIZ","HJB","HKG","HKH","HKM",
"HKR","HMC","HMG","HMS","HOA","HOY","HP3","HPD","HPH","HPO","HPT",
"HRZ","HSA","HSL","HST","HTJ","HVK","HVS","HVT","HVU","HVZ","HYP",
"HYV","IAE","IAM","IAT","IAU","IBB","ICA","ICC","ICD","ICE","ICF",
"ICH","ICS","ICV","ICZ","IDD","IDF","IDH","IDL","IDN","IDO","IDY",
"IEV","IFN","IFV","IHA","IHC","IHK","IHP","IHT","IHZ","IIE","IIH",
"IJA","IJB","IJC","IJH","IJP","IJT","IJV","IJZ","IKL","IKP","ILZ",
"IMF","IML","IOG","IOO","IPB","IPG","IPJ","IPV","IRF","IRO","IRT",
"IRV","IRY","IST","ITP","IUF","IUH","IUT","IUV","IV8","IVF","IVY",
"IYB","IZB","IZF","IZS","JET","KAV","KCV","KDL","KDT","KGM","KHA",
"KHC","KHJ","KHT","KIA","KIB","KID","KIE","KIF","KIS","KKH","KKL",
"KLH","KLS","KLU","KMF","KNJ","KPA","KPC","KPD","KPH","KPP","KPU",
"KRA","KRC","KRF","KRS","KRT","KSA","KSK","KSR","KST","KSV","KTI",
"KTJ","KTM","KTN","KTR","KTS","KTT","KTV","KUB","KUT","KVS","KYA",
"KZL","KZU","LID","LLA","MAC","MAD","MBL","MET","MGH","MJB","MJG",
"MJL","MKG","MMH","MPF","MPK","MPN","MPS","MRI","MTS","MTV","MTX",
"MUT","NAU","NBH","NBZ","NCS","NHP","NHY","NJF","NJG","NJR","NJY",
"NNF","NPH","NRC","NRG","NSA","NSD","NSH","NSK","NTS","NUB","NVB",
"NVT","NVZ","NZH","NZT","OBI","OBP","ODD","ODP","ODS","ODV","OFI",
"OFS","OGD","OHB","OHK","OIL","OIR","OJB","OJK","OJT","OKD","OKP",
"OKT","OLA","OLD","OLE","ONE","ONK","ONN","ONS","OPB","OPD","OPF",
"OPH","OPI","OPL","OSD","OSL","OTJ","OTK","OUD","OVD","PAF","PAL",
"PBI","PBK","PBR","PDD","PDF","PFO","PFS","PHE","PID","PIL","PJL",
"PKF","PPB","PPE","PPF","PPI","PPK","PPN","PPP","PPS","PPT","PPZ",
"PRD","PRH","PRU","PRY","PSL","PUC","PVK","RBA","RBF","RBH","RBI",
"RBK","RBN","RBP","RBR","RBT","RBV","RD1","RDF","RHS","RIK","RJG",
"RKH","RKS","RKV","RPC","RPD","RPG","RPI","RPM","RPP","RPS","RPT",
"RPX","RTD","RTG","RTH","RTP","RUT","SAS","SHE","SOS","SPE","SPN",
"SPR","SPT","ST1","SUA","SUB","SUC","SVB","TAL","TAR","TAU","TBE",
"TBT","TBV","TCA","TCB","TCD","TCF","TDG","TE3","TE4","TEJ","TFF",
"TGA","TGE","TGR","TGX","THD","THT","TI2","TI3","TI4","TI6","TI7",
"TIE","TJF","TJI","TJT","TKF","TLE","TLH","TLZ","TMC","TMG","TMM",
"TMZ","TOT","TPC","TPF","TPJ","TPL","TPP","TPV","TPZ","TRO","TRU",
"TTA","TTE","TUA","TVN","TYH","TZD","TZT","UJA","UP1","UP2","UPD",
"UPH","UPP","USY","VAY","VCY","VFK","YAC","YAK","YAN","YAS","YAY",
"YBE","YBS","YCP","YCY","YDI","YDP","YEF","YFV","YGM","YHB","YHK",
"YHS","YHZ","YJH","YJK","YJY","YKS","YKT","YLC","YLE","YLO","YLY",
"YMD","YNK","YOT","YP4","YPC","YPK","YPL","YPV","YSL","YSU","YTD",
"YTV","YTY","YUB","YUN","YVG","YZC","YZG","YZH","YZK","ZBD","ZBI",
"ZBJ","ZBO","ZCD","ZCK","ZCN","ZDD","ZDZ","ZFB","ZHH","ZJB","ZJI",
"ZJL","ZJV","ZLH","ZMT","ZMY","ZP6","ZP8","ZP9","ZPA","ZPC","ZPE",
"ZPF","ZPG","ZSF","ZSG","ZVO"
]

# SQL Query to fetch fund price data
query = """
SELECT Date, Price
FROM Fund_History
WHERE Fund_Code = ?
ORDER BY Date ASC;
"""

# SQL Insert Query for linearity analysis
insert_query = """
INSERT INTO Linearity_Analysis (Fund_Code, Slope, Intercept, R_Squared, Analysis_Date)
VALUES (?, ?, ?, ?, ?)
"""

# Fetch and analyze data for each fund
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for fund_code in fund_codes:
    try:
        # Fetch data
        data = pd.read_sql_query(query, conn, params=(fund_code,))
        
        if data.empty:
            print(f"No data found for Fund {fund_code}")
            continue
        
        # Convert date to datetime and map to ordinal
        data['Date'] = pd.to_datetime(data['Date'])
        data['Time'] = data['Date'].map(pd.Timestamp.toordinal)
        
        # Perform linear regression
        slope, intercept, r_value, _, _ = linregress(data['Time'], data['Price'])
        
        # R-squared value
        r_squared = r_value ** 2
        
        # Insert analysis into the database
        analysis_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(insert_query, (fund_code, slope, intercept, r_squared, analysis_date))
        
        print(f"Analysis completed for Fund {fund_code}: R-squared = {r_squared:.4f}")
    
    except Exception as e:
        print(f"Error analyzing Fund {fund_code}: {e}")

# Commit changes and close the connection
conn.commit()
conn.close()
