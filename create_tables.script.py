CREATE TABLE Fund_Details (
                              "Code" TEXT PRIMARY KEY,
                              "Title" TEXT,
                              "Category" TEXT,
                              "Rank" TEXT,
                              "Market_Share" TEXT,
                              "ISIN_Code" TEXT,
                              "Start_Time" TEXT,
                              "End_Time" TEXT,
                              "Value_Date" INTEGER,
                              "Back_Value_Date" INTEGER,
                              "Status" TEXT,
                              "KAP_URL" TEXT
);

CREATE TABLE Fund_History (
                              "Fund_Code" TEXT,
                              "Date" DATE,
                              "Timestamp" INTEGER,
                              "Price" REAL,
                              "Market_Cap" REAL,
                              "Number_Of_Shares" REAL,
                              "Number_Of_Investors" INTEGER,
                              FOREIGN KEY ("Fund_Code") REFERENCES Fund_Details("Code")
);

CREATE TABLE Fund_Assets (
                             "Fund_Code" TEXT,
                             "Date" DATE,
                             "Code" TEXT,
                             "Percent" REAL,
                             "Name" TEXT,
                             FOREIGN KEY ("Fund_Code") REFERENCES Fund_Details("Code"),
                             FOREIGN KEY ("Date") REFERENCES Fund_History("Date")
);

