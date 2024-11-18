import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# Database path
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

def calculate_sharpe_ratios(fund_codes, start_date, end_date, risk_free_rate=0.03):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # SQL query to fetch fund data
    query = """
        SELECT Date, Price
        FROM Fund_History
        WHERE Fund_Code = ? AND Date BETWEEN ? AND ?
        ORDER BY Date
    """
    
    # SQL query to insert Sharpe Ratio into the database
    insert_query = """
    INSERT INTO Sharpe_Ratio_Analysis (Fund_Code, Start_Date, End_Date, Sharpe_Ratio, Analysis_Date)
    VALUES (?, ?, ?, ?, ?)
    """
    
    # Iterate through each fund
    for fund_code in fund_codes:
        print(f"Calculating Sharpe Ratio for Fund: {fund_code}")
        
        try:
            # Fetch fund data
            df = pd.read_sql_query(query, conn, params=(fund_code, start_date, end_date))
            
            # Validate data
            if df.empty or len(df) < 2:
                print(f"Insufficient data for Fund: {fund_code}")
                continue
            
            df = df.dropna(subset=['Price'])
            df = df[df['Price'] > 0]
            
            # Calculate daily returns
            df['Return'] = df['Price'].pct_change()
            df = df.dropna(subset=['Return'])  # Drop NaN returns
            
            # Calculate metrics
            avg_return = df['Return'].mean()
            volatility = df['Return'].std()
            
            # Annualize metrics
            trading_days = 252
            annualized_return = avg_return * trading_days
            annualized_volatility = volatility * np.sqrt(trading_days)
            
            # Calculate Sharpe Ratio
            sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
            
            # Insert results into the database
            analysis_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(insert_query, (fund_code, start_date, end_date, sharpe_ratio, analysis_date))
            
            print(f"Sharpe Ratio for {fund_code}: {sharpe_ratio:.2f}")
        
        except Exception as e:
            print(f"Error calculating Sharpe Ratio for {fund_code}: {e}")
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("All calculations completed and saved to the database.")

# Example usage
funds = [
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
calculate_sharpe_ratios(funds, "2024-01-01", "2024-11-15")
