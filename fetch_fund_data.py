import sqlite3
from tefaswrapper import Wrapper, FundType
from datetime import datetime, timedelta


# Initialize TEFAS Wrapper
tefas = Wrapper()

funds = [
    "IVY", "TGR", "GZY", "YZC", "DVT", "IJZ", 
    "GBV", "FLY", "ZFB", "IEV", "RUT", "TFF", 
    "MET", "VCY", "FJB", "ZMY", "RTG", "YPC", 
    "MTX", "JET", "KIB"
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
        current_end_date = current_start_date + timedelta(days=30)
        
        # Ensure we do not exceed the end date or today's date
        if current_end_date > end_date.date():  # Convert end_date to date
            current_end_date = end_date.date()
        if current_end_date > today:
            current_end_date = today
        
        yield current_start_date.strftime("%d.%m.%Y"), current_end_date.strftime("%d.%m.%Y")
        current_start_date = current_end_date + timedelta(days=1)

for fund in funds:
    print(fund)

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

