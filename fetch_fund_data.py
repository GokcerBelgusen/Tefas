import sqlite3
from tefaswrapper import Wrapper, FundType
from datetime import datetime, timedelta
import threading


# Initialize TEFAS Wrapper
tefas = Wrapper()

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

# SQLite database setup
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# Define start and end dates for the year
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

def get_month_date_ranges(start_date, end_date):
    current_start_date = start_date.date()  # Convert start_date to date
    today = datetime.today().date()  # Get today's date as a date object
    
    while current_start_date <= end_date.date() and current_start_date <= today:  # Convert end_date to date
        current_end_date = current_start_date + timedelta(days=90)
        
        # Ensure we do not exceed the end date or today's date
        if current_end_date > end_date.date():  # Convert end_date to date
            current_end_date = end_date.date()
        if current_end_date > today:
            current_end_date = today
        
        yield current_start_date.strftime("%d.%m.%Y"), current_end_date.strftime("%d.%m.%Y")
        current_start_date = current_end_date + timedelta(days=1)

def process(fund):

    # Loop through each month of the year
    for start, end in get_month_date_ranges(start_date, end_date):
        print(f"Fetching data from {start} to {end}")
            
        result = []

        # Fetch data from TEFAS
        result = tefas.fetch(fund, start, end)

        # Check the structure of the result
        # print(result)

        # Check if 'NJR' is in the result and if it's a Fund object
        if fund in result:
            fund_data = result[fund]
            
            # Print the attributes of the Fund object to understand its structure
            print(fund_data.__dict__)  # This will show the object's attributes

            # Now safely access the 'code' attribute
            print(fund_data.code)  # Assuming 'code' is an attribute of the Fund object

            # SQLite database operations
            try:
                # Establish a connection to the SQLite database
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()

                    # Extract fund details
                    fund_details = (
                        fund_data.code,  # 'code' attribute
                        fund_data.title,  # Assuming 'title' is an attribute
                        fund_data.category,  # Assuming 'category' is an attribute
                        fund_data.rank,  # Assuming 'rank' is an attribute
                        fund_data.market_share,  # Assuming 'market_share' is an attribute
                        fund_data.isin_code,  # Assuming 'isin_code' is an attribute
                        fund_data.start_time,  # Assuming 'start_time' is an attribute
                        fund_data.end_time,  # Assuming 'end_time' is an attribute
                        int(fund_data.value_date or 0),  # Handle missing value_date gracefully
                        int(fund_data.back_value_date or 0),  # Handle missing back_value_date gracefully
                        fund_data.status,  # Assuming 'status' is an attribute
                        fund_data.kap_url  # Assuming 'kap_url' is an attribute
                    )

                    # Insert Fund Details into the database
                    cursor.execute("""
                        INSERT OR IGNORE INTO Fund_Details (
                            "Code", "Title", "Category", "Rank", "Market_Share", "ISIN_Code",
                            "Start_Time", "End_Time", "Value_Date", "Back_Value_Date", "Status", "KAP_URL"
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, fund_details)
                    print(f"Fund details inserted: {fund_details}")

                    # Extract and insert fund history (if available)
                    for history in fund_data.history:
                        history_date = datetime.strptime(history.date, "%d-%m-%Y").date().isoformat()
                        
                        fund_history = (
                            fund_data.code,  # 'code' attribute
                            history_date,  # Assuming 'date' is an attribute of history
                            history.timestamp,  # Assuming 'timestamp' is an attribute of history
                            history.price,  # Assuming 'price' is an attribute of history
                            history.market_cap,  # Assuming 'market_cap' is an attribute of history
                            history.number_of_shares,  # Assuming 'number_of_shares' is an attribute of history
                            history.number_of_investors  # Assuming 'number_of_investors' is an attribute of history
                        )

                        cursor.execute("""
                            INSERT OR IGNORE INTO Fund_History (
                                "Fund_Code", "Date", "Timestamp", "Price", "Market_Cap",
                                "Number_Of_Shares", "Number_Of_Investors"
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, fund_history)
                        print(f"Fund history inserted: {fund_history}")

                        # Extract and insert fund assets (if available)
                        for asset in history.assets:
                            fund_asset = (
                                fund_data.code,  # 'code' attribute
                                history_date,  # 'date' attribute from history
                                asset.code,  # Assuming 'code' is an attribute of asset
                                asset.percent,  # Assuming 'percent' is an attribute of asset
                                asset.name  # Assuming 'name' is an attribute of asset
                            )

                            cursor.execute("""
                                INSERT OR IGNORE INTO Fund_Assets (
                                    "Fund_Code", "Date", "Code", "Percent", "Name"
                                ) VALUES (?, ?, ?, ?, ?)
                            """, fund_asset)
                            print(f"Fund asset inserted: {fund_asset}")

                    # Commit the transaction
                    conn.commit()
                    print("Data has been successfully inserted into the database.")

            except sqlite3.Error as e:
                print(f"SQLite error: {e}")
            except Exception as e:
                print(f"An error occurred: {e}")
        else:
            print(f"No data found for fund {fund}")
    

# Create a function to process funds in parallel
def process_in_parallel(funds, num_threads=5):
    # Function to divide the work among threads
    def thread_worker(fund_list):
        for fund in fund_list:
            process(fund)

    # Split funds into chunks to distribute among threads
    chunk_size = len(funds) // num_threads
    threads = []

    # Create threads and assign chunks of funds to each thread
    for i in range(num_threads):
        start = i * chunk_size
        # Ensure the last chunk contains any remaining funds
        end = start + chunk_size if i < num_threads - 1 else len(funds)
        fund_chunk = funds[start:end]

        thread = threading.Thread(target=thread_worker, args=(fund_chunk,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()


# Call the function to process funds in parallel
process_in_parallel(funds, num_threads=5)




