import requests
import sqlite3
import time
import pandas as pd
import os
import json

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time


class community_solarDatabase:
    def __init__(self):
        self.conn = sqlite3.connect("community_solar.db")

    ### Start of locations table methods ###

    def get_locations_data(self):
        # Source page: https://www.indianamap.org/datasets/INMap::address-points-of-indiana-current/explore?location=39.705743%2C-86.396120%2C7.96
        url = "https://hub.arcgis.com/api/download/v1/items/9b222d07cc164eb384a24742cbf1d274/csv?redirect=false&layers=0"

        while True:
            response = requests.get(url)
            data = response.json()
            if data.get("status") == "Completed":
                result_url = data.get("resultUrl")
                print(
                    "Status is 'Completed'. Proceeding to download CSV from:",
                    result_url,
                )
                break
            else:
                print(f"Status is '{data.get('status')}'. Waiting and retrying...")
                time.sleep(5)

        csv_response = requests.get(result_url)
        if csv_response.ok:
            #self.process_locations_data(csv_response)
            with open("locations_data.csv", "wb") as file:
                file.write(csv_response.content)
            print("CSV file downloaded and saved as 'locations_data.csv'.")
        else:
            print("Failed to download CSV. Status code:", csv_response.status_code)

    def process_locations_data(self, csv_response):
        with open("locations_data.csv", "wb") as file:
            file.write(csv_response.content)
        df = pd.read_csv("locations_data.csv")
        columns_to_keep = [
            "latitude",
            "longitude",
            "dlgf_prop_class_code",
            "add_full",
            "geocity",
            "geozip",
            "geocounty",
            "geostate",
        ]
        df = df[columns_to_keep]
        df.to_csv("locations_data.csv", index=False)

    def check_locations_table_exists(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='LOCATIONS';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'LOCATIONS' exists.")
        else:
            print("Table 'LOCATIONS' does not exist.")
            self.create_locations_table()
        cursor.close()
        return result

    def create_locations_table(self):
        cursor = self.conn.cursor()
        # Read the CSV file to get the header names (ensure the file name matches)
        df = pd.read_csv("locations_data.csv", low_memory=False)
        header = df.columns
        columns_definitions = []
        columns_definitions.append(f'"location_id" INTEGER PRIMARY KEY AUTOINCREMENT')
        column_type_mapping = {
            "latitude": "REAL",
            "longitude": "REAL",
            "dlgf_prop_class_code": "INTEGER"
        }
        for col in header:
            col_type = column_type_mapping.get(col, "TEXT")
            col_definition = f'"{col}" {col_type}'
            columns_definitions.append(col_definition)
        columns_sql = ", ".join(columns_definitions)
        table_name = "LOCATIONS"
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        print("Creating table with query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        self.conn.commit()
        cursor.close()

    def insert_locations_data(self):
        cursor = self.conn.cursor()
        df = pd.read_csv("locations_data.csv", low_memory=False)
        df.fillna("", inplace=True)
        print("Columns:", df.columns.tolist())
        header = df.columns
        table_name = "LOCATIONS"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows:")
        for index, row in df.iterrows():
            values = row.tolist()
            try:
                cursor.execute(insert_query, values)
                if index % 1000 == 0:
                    print(f"Inserted row {index}")
            except sqlite3.Error as err:
                print(values)
                print("Error inserting row:", err)
        self.conn.commit()
        cursor.close()
        print("Data update completed successfully.")

    ### End of locations table methods ###

    ### Start of nonprofits table methods ###

    def get_nonprofit_data(self):
        options = Options()
        options.headless = True 
        service = Service(executable_path=ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=options)

        try:
            url = "https://www.stats.indiana.edu/nonprofit/inp.aspx"
            driver.get(url)

            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))

            time.sleep(2)

            rows = table.find_elements(By.TAG_NAME, "tr")
            data = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    cells = row.find_elements(By.TAG_NAME, "th")
                if cells:
                    data.append([cell.text.strip() for cell in cells])

            with open("nonprofit_data.csv", "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerows(data)

            print("Data scraped successfully and saved to nonprofit_data.csv")

        except Exception as e:
            print("An error occurred:", e)
        finally:
            driver.quit()

    def check_nonprofits_table_exists(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='NONPROFITS';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'NONPROFITS' exists.")
        else:
            print("Table 'NONPROFITS' does not exist.")
            self.create_nonprofits_table()
        cursor.close()
        return result

    def create_nonprofits_table(self):
        cursor = self.conn.cursor()
        df = pd.read_csv("nonprofit_data.csv", low_memory=False)
        header = df.columns
        columns_definitions = []
        for col in header:
            col_definition = f'"{col}" TEXT'
            columns_definitions.append(col_definition)
        columns_sql = ", ".join(columns_definitions)
        table_name = "NONPROFITS"
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        print("Creating table with query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        self.conn.commit()
        cursor.close()

    def parse_nonprofit_data(self):
        named_download_dir = os.path.abspath("downloads/named/")
        files = os.listdir(named_download_dir)
        dfs = []
        for file in files:
            county_name = os.path.basename(file).split(".")[0]
            df = pd.read_csv(named_download_dir + "/" + file, skiprows=3)
            df["county"] = county_name
            dfs.append(df)
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv("nonprofit_data.csv", index=False)

    def insert_nonprofit_data(self):
        cursor = self.conn.cursor()
        df = pd.read_csv("nonprofit_data.csv", low_memory=False)
        df.fillna("", inplace=True)
        print("Columns:", df.columns.tolist())
        header = df.columns
        table_name = "NONPROFITS"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows as a test:")
        for index, row in df.iterrows():
            values = row.tolist()
            try:
                cursor.execute(insert_query, values)
                if index % 1000 == 0:
                    print(f"Inserted row {index}")
            except sqlite3.Error as err:
                print("Error inserting row:", err)

        self.conn.commit()
        cursor.close()
        print("Data update completed successfully.")

    ### End of nonprofits table methods ###

    ### Start of Google Solar API methods ###

    def check_google_solar_table_exists(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='GOOGLE_SOLAR';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'GOOGLE_SOLAR' exists.")
        else:
            print("Table 'GOOGLE_SOLAR' does not exist.")
            self.create_google_solar_table()
        cursor.close()
        return result

    def create_google_solar_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS GOOGLE_SOLAR (
                solar_id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id INTEGER NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                max_panel_count INTEGER NOT NULL,
                yearly_energy_production FLOAT NOT NULL,
                date_added DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """
        )
        self.conn.commit()
        cursor.close()

    def get_solar_data(self, latitude, longitude):
        api_key = open("google_api_key.txt", "r").read().strip()
        api_url = f"https://solar.googleapis.com/v1/buildingInsights:findClosest?location.latitude={latitude}&location.longitude={longitude}&requiredQuality=HIGH&key="+api_key
        response = requests.get(api_url)
        solar_data = response.json()
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
        else:
            print("Data fetched successfully")
            with open(f"solar_data_{latitude}_{longitude}.json", "w") as f:
                json.dump(solar_data, f, indent=4)
            return solar_data

    def process_solar_data(self, solar_data):
        max_panel_count = solar_data["solarPotential"]["solarPanelConfigs"][-1]["panelsCount"]
        yearly_energy_production = solar_data["solarPotential"]["solarPanelConfigs"][-1]["yearlyEnergyDcKwh"]
        return max_panel_count, yearly_energy_production

    def insert_solar_data(
        self,
        location_id,
        latitude,
        longitude,
        max_panel_count,
        yearly_energy_production,
    ):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO GOOGLE_SOLAR (location_id, latitude, longitude, max_panel_count, yearly_energy_production)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                location_id,
                latitude,
                longitude,
                max_panel_count,
                yearly_energy_production,
            ),
        )
        self.conn.commit()
        cursor.close()

    def demo_solar_data(self):
        df = pd.read_sql_query(
            "SELECT * FROM LOCATIONS WHERE DLGF_PROP_CLASS_CODE = 645.0 LIMIT 100000",
            self.conn,
        )
        df.head(4)
        row = df.iloc[2]
        print(row)
        location_id = int(row["location_id"])
        latitude = row["latitude"]
        longitude = row["longitude"]

        solar_data = self.get_solar_data(latitude, longitude)

        max_panel_count, yearly_energy_production = self.process_solar_data(solar_data)
        self.insert_solar_data(
            location_id, latitude, longitude, max_panel_count, yearly_energy_production
        )

        print(f"Location ID: {location_id}")
        print(f"Latitude: {latitude}")
        print(f"Longitude: {longitude}")
        print(f"Max Panel Count: {max_panel_count}")
        print(f"Yearly Production: {yearly_energy_production} DcKwh")

        df = pd.read_sql_query("SELECT * FROM GOOGLE_SOLAR LIMIT 5", self.conn)
        print(df.head(4))

    ### End of Google Solar API methods ###

    def clear_database(self):
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS GOOGLE_SOLAR;")
        cursor.execute("DROP TABLE IF EXISTS LOCATIONS;")
        cursor.execute("DROP TABLE IF EXISTS NONPROFITS;")
        self.conn.commit()
        cursor.close()
        print("Database cleared.")

    def show_db_structure(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        if not tables:
            print("No tables found in the database.")
            return
        for table in tables:
            table_name = table[0]
            print(f"Table: {table_name}")
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            if columns:
                print(" Columns:")
                for col in columns:
                    cid, name, col_type, notnull, dflt_value, pk = col
                    print(f"  - {name} ({col_type}){' PRIMARY KEY' if pk else ''}{' NOT NULL' if notnull else ''}")
            else:
                print(" No columns found.")
            print() 

    def create_database_and_build(self):
        # Gets the newest address data from the Indiana map and saves it to a CSV file
        # Creates the LOCATIONS table and inserts the address data into the table for every location
        self.get_locations_data()
        self.check_locations_table_exists()
        self.insert_locations_data()

        # Creates the NONPROFITS table and inserts the nonprofit data into the table for every location
        #self.get_nonprofit_data() # Only run this if need be, will take awhile to scrape the data
        self.check_nonprofits_table_exists()
        self.parse_nonprofit_data()
        self.insert_nonprofit_data()

        # Creates the GOOGLE_SOLAR table
        self.check_google_solar_table_exists()
        self.demo_solar_data()

        # Show the database structure
        self.show_db_structure()

        self.conn.close()


if __name__ == "__main__":

    db = community_solarDatabase()
    db.create_database_and_build()
