import json
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# Load environment variables from .env file
load_dotenv()


# db connection
def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),
        )
        print("Connected to PostgreSQL database!")
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None


# --------------------------------Aggregated Insurance Data--------------------------------
def agg_insurance_data():
    path = "data/aggregated/insurance/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states")

    insurance_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Insurance_type": [],
        "Insurance_count": [],
        "Insurance_amount": [],
    }

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = os.path.join(p_i, j)
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = os.path.join(p_j, k)

                try:
                    with open(p_k, "r") as Data:
                        D = json.load(Data)

                    if (
                        "transactionData" in D.get("data", {})
                        and D["data"]["transactionData"]
                    ):
                        for z in D["data"]["transactionData"]:
                            if z.get("paymentInstruments"):
                                Name = z["name"]
                                count = z["paymentInstruments"][0].get("count", 0)
                                amount = z["paymentInstruments"][0].get("amount", 0.0)

                                insurance_data["Insurance_type"].append(Name)
                                insurance_data["Insurance_count"].append(count)
                                insurance_data["Insurance_amount"].append(amount)
                                insurance_data["State"].append(i)
                                insurance_data["Year"].append(int(j))
                                insurance_data["Quarter"].append(int(k.strip(".json")))

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    Agg_Insurance = pd.DataFrame(insurance_data)
    print(f"Successfully created DataFrame with {len(Agg_Insurance)} rows")
    return Agg_Insurance

def insurance_table(conn):
    """Create table in PostgreSQL if it doesn't exist"""
    try:
        cursor = conn.cursor()

        create_insurance_query = """
           CREATE TABLE IF NOT EXISTS agg_insurance (
              id SERIAL PRIMARY KEY,
              State VARCHAR(100),
              Year INT,
              Quarter INT,
              Insurance_type VARCHAR(100),
              Insurance_count BIGINT,
              Insurance_amount NUMERIC(30,2)
            )
        """

        cursor.execute(create_insurance_query)
        conn.commit()
        print("Table created successfully!")
        return True

    except Exception as e:
        print(f"Table creation error: {e}")
        return False

def save_to_postgres(df, conn):
    try:
        # Clear existing data
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE agg_insurance")
        conn.commit()

        # Insert new data
        for index, row in df.iterrows():
            insert_query = """
                INSERT INTO agg_insurance (State, Year, Quarter, Insurance_type, Insurance_count, Insurance_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(
                insert_query,
                (
                    row["State"],
                    row["Year"],
                    row["Quarter"],
                    row["Insurance_type"],
                    row["Insurance_count"],
                    row["Insurance_amount"],
                ),
            )

        conn.commit()
        print("Data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"Save error: {e}")
        return False

def show_data_from_postgres(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM agg_insurance")
        count = cursor.fetchone()[0]
        print(f"Total rows in database: {count}")

        cursor.execute("SELECT * FROM agg_insurance LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 rows:")
        for row in rows:
            print(
                f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, Type: {row[4]}, Count: {row[5]}, Amount: {row[6]}"
            )

    except Exception as e:
        print(f"Read error: {e}")


# --------------------------------Aggregated Transaction Data--------------------------------
def agg_transaction_data():
    path = "data/aggregated/transaction/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for transaction data")

    transaction_data = {'State':[], 'Year':[], 'Quarter':[], 'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = os.path.join(p_i, j)
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = os.path.join(p_j, k)

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    if (
                        "transactionData" in D.get("data", {})
                        and D["data"]["transactionData"]
                    ):
                        for z in D["data"]["transactionData"]:
                            if z.get("paymentInstruments"):
                                Name = z["name"]
                                count = z["paymentInstruments"][0].get("count", 0)
                                amount = z["paymentInstruments"][0].get("amount", 0.0)

                                transaction_data["Transaction_type"].append(Name)
                                transaction_data["Transaction_count"].append(count)
                                transaction_data["Transaction_amount"].append(amount)
                                transaction_data["State"].append(i)
                                transaction_data["Year"].append(j)
                                transaction_data["Quarter"].append(int(k.strip('.json')))

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    Agg_Transaction = pd.DataFrame(transaction_data)
    print(f"Successfully created Transaction DataFrame with {len(Agg_Transaction)} rows")
    return Agg_Transaction

def transaction_table(conn):
    try:
        cursor = conn.cursor()

        create_transaction_query = '''
            CREATE TABLE IF NOT EXISTS agg_transaction (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year VARCHAR(10),
                Quarter INTEGER,
                Transaction_type VARCHAR(100),
                Transaction_count BIGINT,
                Transaction_amount NUMERIC(30,2)
            )
        '''

        cursor.execute(create_transaction_query)
        conn.commit()
        print("Transaction table created successfully!")
        return True

    except Exception as e:
        print(f"Transaction table creation error: {e}")
        return False

def agg_transaction_db_save(df, conn):
    try:
        # Clear existing data
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE agg_transaction")
        conn.commit()

        # Insert new data
        for index, row in df.iterrows():
            insert_query = '''
                INSERT INTO agg_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_query, (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_count"],
                row["Transaction_amount"],
            ),
        )

        conn.commit()
        print("Transaction data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"Transaction save error: {e}")
        return False

def agg_transaction_db_show(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM agg_transaction")
        count = cursor.fetchone()[0]
        print(f"Total transaction rows in database: {count}")

        cursor.execute("SELECT * FROM agg_transaction LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 transaction rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, Type: {row[4]}, Count: {row[5]}, Amount: {row[6]}")

    except Exception as e:
        print(f"Transaction read error: {e}")


# --------------------------------Aggregated User Data--------------------------------
def agg_user_data():
    path = "data/aggregated/user/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for user data")

    # Data for aggregated user information
    user_aggregated_data = {'State':[], 'Year':[], 'Quarter':[], 'Registered_Users':[], 'App_Opens':[]}

    # Data for device-specific user information
    user_device_data = {'State':[], 'Year':[], 'Quarter':[], 'Brand':[], 'User_Count':[], 'Percentage':[]}

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = os.path.join(p_i, j)
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = os.path.join(p_j, k)

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    if not D or 'data' not in D or not D['data']:
                        continue

                    # Extract aggregated
                    agg_data = D['data'].get('aggregated', {})
                    if agg_data:
                        user_aggregated_data['State'].append(i)
                        user_aggregated_data['Year'].append(int(j))
                        user_aggregated_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        user_aggregated_data['Registered_Users'].append(agg_data.get('registeredUsers', 0))
                        user_aggregated_data['App_Opens'].append(agg_data.get('appOpens', 0))

                    # Extract device-specific
                    users_by_device = D['data'].get('usersByDevice')
                    if users_by_device and isinstance(users_by_device, list):
                        for device in users_by_device:
                            if device and isinstance(device, dict):
                                brand = device.get('brand')
                                count = device.get('count')
                                percentage = device.get('percentage')
                                if brand is not None and count is not None and percentage is not None:
                                    user_device_data['State'].append(i)
                                    user_device_data['Year'].append(int(j))
                                    user_device_data['Quarter'].append(int(os.path.splitext(k)[0]))
                                    user_device_data['Brand'].append(brand)
                                    user_device_data['User_Count'].append(count)
                                    user_device_data['Percentage'].append(percentage)

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrames
    Agg_User_Aggregated = pd.DataFrame(user_aggregated_data)
    Agg_User_Device = pd.DataFrame(user_device_data)

    print(f"Successfully created User Aggregated DataFrame with {len(Agg_User_Aggregated)} rows")
    print(f"Successfully created User Device DataFrame with {len(Agg_User_Device)} rows")

    return Agg_User_Aggregated, Agg_User_Device

def create_user_tables(conn):
    """Create user tables in PostgreSQL if they don't exist"""
    try:
        # Rollback any pending transaction to start fresh
        conn.rollback()
        cursor = conn.cursor()

        # Table for aggregated user data
        create_user_aggregated_query = '''
            CREATE TABLE IF NOT EXISTS agg_user (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                Registered_Users BIGINT,
                App_Opens BIGINT
            )
        '''

        # Table for device-specific user data
        create_user_device_query = '''
            CREATE TABLE IF NOT EXISTS agg_user_device (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                Brand VARCHAR(100),
                User_Count BIGINT,
                Percentage NUMERIC(12,6)
            )
        '''

        cursor.execute(create_user_aggregated_query)
        cursor.execute(create_user_device_query)
        conn.commit()
        print("User tables created successfully!")
        return True

    except Exception as e:
        print(f"User table creation error: {e}")
        # Rollback on error
        conn.rollback()
        return False

def save_user_data_to_postgres(aggregated_df, device_df, conn):
    try:
        # Rollback any pending transaction to start fresh
        conn.rollback()
        
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE agg_user")
            cursor.execute("TRUNCATE TABLE agg_user_device")

            agg_insert_query = '''
                INSERT INTO agg_user (State, Year, Quarter, Registered_Users, App_Opens)
                VALUES (%s, %s, %s, %s, %s)
            '''
            execute_batch(cursor, agg_insert_query, aggregated_df.values.tolist())

            device_insert_query = '''
                INSERT INTO agg_user_device (State, Year, Quarter, Brand, User_Count, Percentage)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''
            execute_batch(cursor, device_insert_query, device_df.values.tolist())

        conn.commit()
        print("User data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"User data save error: {e}")
        # Rollback on error
        conn.rollback()
        return False

def show_user_data_from_postgres(conn):
    try:
        cursor = conn.cursor()

        # Show aggregated user data
        cursor.execute("SELECT COUNT(*) FROM agg_user")
        count = cursor.fetchone()[0]
        print(f"Total aggregated user rows in database: {count}")

        cursor.execute("SELECT * FROM agg_user LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 aggregated user rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, Users: {row[4]}, App Opens: {row[5]}")

        # Show device-specific user data
        cursor.execute("SELECT COUNT(*) FROM agg_user_device")
        count = cursor.fetchone()[0]
        print(f"\nTotal device-specific user rows in database: {count}")

        cursor.execute("SELECT * FROM agg_user_device LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 device-specific user rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, Brand: {row[4]}, Count: {row[5]}, Percentage: {row[6]}")

    except Exception as e:
        print(f"User data read error: {e}")


# --------------------------------Top Insurance Data--------------------------------
def top_insurance_data():
    path = "data/top/insurance/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for top insurance data")

    # Data for top districts
    top_district_data = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'District_Count':[], 'District_Amount':[]}

    # Data for top pincodes
    top_pincode_data = {'State':[], 'Year':[], 'Quarter':[], 'Pincode':[], 'Pincode_Count':[], 'Pincode_Amount':[]}

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = os.path.join(p_i, j)
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = os.path.join(p_j, k)

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    # Extract top districts data
                    for district in D['data'].get('districts', []):
                        top_district_data['State'].append(i)
                        top_district_data['Year'].append(int(j))
                        top_district_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        top_district_data['District'].append(district['entityName'])
                        top_district_data['District_Count'].append(district['metric']['count'])
                        top_district_data['District_Amount'].append(district['metric']['amount'])

                    # Extract top pincodes data
                    for pincode in D['data'].get('pincodes', []):
                        top_pincode_data['State'].append(i)
                        top_pincode_data['Year'].append(int(j))
                        top_pincode_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        top_pincode_data['Pincode'].append(pincode['entityName'])
                        top_pincode_data['Pincode_Count'].append(pincode['metric']['count'])
                        top_pincode_data['Pincode_Amount'].append(pincode['metric']['amount'])

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrames
    Top_Insurance_District = pd.DataFrame(top_district_data)
    Top_Insurance_Pincode = pd.DataFrame(top_pincode_data)

    print(f"✅ Created Top Insurance District DataFrame with {len(Top_Insurance_District)} rows")
    print(f"✅ Created Top Insurance Pincode DataFrame with {len(Top_Insurance_Pincode)} rows")

    return Top_Insurance_District, Top_Insurance_Pincode

def create_top_insurance_tables(conn):
    """Create top insurance tables in PostgreSQL if they don't exist"""
    try:
        conn.rollback()
        cursor = conn.cursor()

        # Table for top districts
        create_top_district_query = '''
            CREATE TABLE IF NOT EXISTS top_insurance_district (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                District VARCHAR(100),
                District_Count BIGINT,
                District_Amount NUMERIC(30,2)
            )
        '''

        # Table for top pincodes
        create_top_pincode_query = '''
            CREATE TABLE IF NOT EXISTS top_insurance_pincode (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                Pincode VARCHAR(10),
                Pincode_Count BIGINT,
                Pincode_Amount NUMERIC(30,2)
            )
        '''

        cursor.execute(create_top_district_query)
        cursor.execute(create_top_pincode_query)
        conn.commit()
        print("✅ Top insurance tables created successfully!")
        return True

    except Exception as e:
        print(f"❌ Top insurance table creation error: {e}")
        return False

def save_top_insurance_to_postgres(district_df, pincode_df, conn):
    try:
        conn.rollback()
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("TRUNCATE TABLE top_insurance_district")
        cursor.execute("TRUNCATE TABLE top_insurance_pincode")

        # Insert top district data
        insert_district_query = '''
            INSERT INTO top_insurance_district (State, Year, Quarter, District, District_Count, District_Amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        execute_batch(cursor, insert_district_query, district_df.values.tolist())

        # Insert top pincode data
        insert_pincode_query = '''
            INSERT INTO top_insurance_pincode (State, Year, Quarter, Pincode, Pincode_Count, Pincode_Amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        execute_batch(cursor, insert_pincode_query, pincode_df.values.tolist())

        conn.commit()
        print("✅ Top insurance data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"❌ Top insurance save error: {e}")
        return False

def show_top_insurance_from_postgres(conn):
    try:
        cursor = conn.cursor()

        # Show top district data
        cursor.execute("SELECT COUNT(*) FROM top_insurance_district")
        count = cursor.fetchone()[0]
        print(f"📊 Total top insurance district rows: {count}")

        cursor.execute("SELECT * FROM top_insurance_district LIMIT 5")
        rows = cursor.fetchall()
        print("First 5 top insurance district rows:")
        for row in rows:
            print(row)

        # Show top pincode data
        cursor.execute("SELECT COUNT(*) FROM top_insurance_pincode")
        count = cursor.fetchone()[0]
        print(f"\n📊 Total top insurance pincode rows: {count}")

        cursor.execute("SELECT * FROM top_insurance_pincode LIMIT 5")
        rows = cursor.fetchall()
        print("First 5 top insurance pincode rows:")
        for row in rows:
            print(row)

    except Exception as e:
        print(f"❌ Top insurance read error: {e}")


# --------------------------------Top Transaction Data--------------------------------

def top_transaction_data():
    path = "data/top/transaction/country/india/state/"
    
    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for top transaction data")

    # Data for top districts
    top_district_data = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'District_Count':[], 'District_Amount':[]}
    
    # Data for top pincodes
    top_pincode_data = {'State':[], 'Year':[], 'Quarter':[], 'Pincode':[], 'Pincode_Count':[], 'Pincode_Amount':[]}

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = os.path.join(p_i, j)
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = os.path.join(p_j, k)

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    # Extract top districts data
                    for district in D['data'].get('districts', []):
                        top_district_data['State'].append(i)
                        top_district_data['Year'].append(int(j))
                        top_district_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        top_district_data['District'].append(district['entityName'])
                        top_district_data['District_Count'].append(district['metric']['count'])
                        top_district_data['District_Amount'].append(district['metric']['amount'])

                    # Extract top pincodes data
                    for pincode in D['data'].get('pincodes', []):
                        top_pincode_data['State'].append(i)
                        top_pincode_data['Year'].append(int(j))
                        top_pincode_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        top_pincode_data['Pincode'].append(pincode['entityName'])
                        top_pincode_data['Pincode_Count'].append(pincode['metric']['count'])
                        top_pincode_data['Pincode_Amount'].append(pincode['metric']['amount'])

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrames
    Top_Transaction_District = pd.DataFrame(top_district_data)
    Top_Transaction_Pincode = pd.DataFrame(top_pincode_data)

    print(f"✅ Created Top Transaction District DataFrame with {len(Top_Transaction_District)} rows")
    print(f"✅ Created Top Transaction Pincode DataFrame with {len(Top_Transaction_Pincode)} rows")

    return Top_Transaction_District, Top_Transaction_Pincode

def create_top_transaction_tables(conn):
    """Create top transaction tables in PostgreSQL if they don't exist"""
    try:
        conn.rollback()  # Reset transaction state
        cursor = conn.cursor()

        # Table for top districts
        create_top_district_query = '''
            CREATE TABLE IF NOT EXISTS top_transaction_district (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                District VARCHAR(100),
                District_Count BIGINT,
                District_Amount NUMERIC(30,2)
            )
        '''

        # Table for top pincodes
        create_top_pincode_query = '''
            CREATE TABLE IF NOT EXISTS top_transaction_pincode (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                Pincode VARCHAR(20),
                Pincode_Count BIGINT,
                Pincode_Amount NUMERIC(30,2)
            )
        '''

        cursor.execute(create_top_district_query)
        cursor.execute(create_top_pincode_query)
        conn.commit()
        print("✅ Top transaction tables created successfully!")
        return True

    except Exception as e:
        print(f"❌ Top transaction table creation error: {e}")
        conn.rollback()
        return False

def save_top_transaction_to_postgres(district_df, pincode_df, conn):
    try:
        conn.rollback()  # Reset transaction state
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("TRUNCATE TABLE top_transaction_district")
        cursor.execute("TRUNCATE TABLE top_transaction_pincode")

        # Insert top district data
        insert_district_query = '''
            INSERT INTO top_transaction_district (State, Year, Quarter, District, District_Count, District_Amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        execute_batch(cursor, insert_district_query, district_df.values.tolist())

        # Insert top pincode data
        insert_pincode_query = '''
            INSERT INTO top_transaction_pincode (State, Year, Quarter, Pincode, Pincode_Count, Pincode_Amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        execute_batch(cursor, insert_pincode_query, pincode_df.values.tolist())

        conn.commit()
        print("✅ Top transaction data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"❌ Top transaction save error: {e}")
        conn.rollback()
        return False

def show_top_transaction_from_postgres(conn):
    try:
        conn.rollback()  # Reset transaction state
        cursor = conn.cursor()

        # Show top district data
        cursor.execute("SELECT COUNT(*) FROM top_transaction_district")
        count = cursor.fetchone()[0]
        print(f"📊 Total top transaction district rows: {count}")

        cursor.execute("SELECT * FROM top_transaction_district LIMIT 5")
        rows = cursor.fetchall()
        print("First 5 top transaction district rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, District: {row[4]}, Count: {row[5]}, Amount: {row[6]}")

        # Show top pincode data
        cursor.execute("SELECT COUNT(*) FROM top_transaction_pincode")
        count = cursor.fetchone()[0]
        print(f"\n📊 Total top transaction pincode rows: {count}")

        cursor.execute("SELECT * FROM top_transaction_pincode LIMIT 5")
        rows = cursor.fetchall()
        print("First 5 top transaction pincode rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, Pincode: {row[4]}, Count: {row[5]}, Amount: {row[6]}")

    except Exception as e:
        print(f"❌ Top transaction read error: {e}")
        conn.rollback()


# --------------------------------Top User Data--------------------------------

def top_user_data():
    path = "data/top/user/country/india/state/"
    
    if not os.path.exists(path):
        print("Path not found!")
        return None, None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for top user data")

    # Data for top districts
    top_district_data = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Registered_Users':[]}
    
    # Data for top pincodes
    top_pincode_data = {'State':[], 'Year':[], 'Quarter':[], 'Pincode':[], 'Registered_Users':[]}

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = os.path.join(p_i, j)
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = os.path.join(p_j, k)

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    # Extract top districts data
                    for district in D['data'].get('districts', []):
                        top_district_data['State'].append(i)
                        top_district_data['Year'].append(int(j))
                        top_district_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        top_district_data['District'].append(district['name'])
                        top_district_data['Registered_Users'].append(district['registeredUsers'])

                    # Extract top pincodes data
                    for pincode in D['data'].get('pincodes', []):
                        top_pincode_data['State'].append(i)
                        top_pincode_data['Year'].append(int(j))
                        top_pincode_data['Quarter'].append(int(os.path.splitext(k)[0]))
                        top_pincode_data['Pincode'].append(pincode['name'])
                        top_pincode_data['Registered_Users'].append(pincode['registeredUsers'])

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrames
    Top_User_District = pd.DataFrame(top_district_data)
    Top_User_Pincode = pd.DataFrame(top_pincode_data)

    print(f"✅ Created Top User District DataFrame with {len(Top_User_District)} rows")
    print(f"✅ Created Top User Pincode DataFrame with {len(Top_User_Pincode)} rows")

    return Top_User_District, Top_User_Pincode

def create_top_user_tables(conn):
    """Create top user tables in PostgreSQL if they don't exist"""
    try:
        conn.rollback()  # Reset transaction state
        cursor = conn.cursor()

        # Table for top user districts
        create_top_district_query = '''
            CREATE TABLE IF NOT EXISTS top_user_district (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                District VARCHAR(100),
                Registered_Users BIGINT
            )
        '''

        # Table for top user pincodes
        create_top_pincode_query = '''
            CREATE TABLE IF NOT EXISTS top_user_pincode (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                Pincode VARCHAR(20),
                Registered_Users BIGINT
            )
        '''

        cursor.execute(create_top_district_query)
        cursor.execute(create_top_pincode_query)
        conn.commit()
        print("Top user tables created successfully!")
        return True

    except Exception as e:
        print(f"❌ Top user table creation error: {e}")
        conn.rollback()
        return False

def save_top_user_to_postgres(district_df, pincode_df, conn):
    try:
        conn.rollback()  # Reset transaction state
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("TRUNCATE TABLE top_user_district")
        cursor.execute("TRUNCATE TABLE top_user_pincode")

        # Insert top district data
        insert_district_query = '''
            INSERT INTO top_user_district (State, Year, Quarter, District, Registered_Users)
            VALUES (%s, %s, %s, %s, %s)
        '''
        execute_batch(cursor, insert_district_query, district_df.values.tolist())

        # Insert top pincode data
        insert_pincode_query = '''
            INSERT INTO top_user_pincode (State, Year, Quarter, Pincode, Registered_Users)
            VALUES (%s, %s, %s, %s, %s)
        '''
        execute_batch(cursor, insert_pincode_query, pincode_df.values.tolist())

        conn.commit()
        print("✅ Top user data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"❌ Top user save error: {e}")
        conn.rollback()
        return False

def show_top_user_from_postgres(conn):
    try:
        conn.rollback()  # Reset transaction state
        cursor = conn.cursor()

        # Districts
        cursor.execute("SELECT COUNT(*) FROM top_user_district")
        count = cursor.fetchone()[0]
        print(f"📊 Total top user district rows: {count}")

        cursor.execute("SELECT * FROM top_user_district LIMIT 5")
        rows = cursor.fetchall()
        print("First 5 top user district rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, "
                  f"District: {row[4]}, Users: {row[5]}")

        # Pincodes
        cursor.execute("SELECT COUNT(*) FROM top_user_pincode")
        count = cursor.fetchone()[0]
        print(f"\n📊 Total top user pincode rows: {count}")

        cursor.execute("SELECT * FROM top_user_pincode LIMIT 5")
        rows = cursor.fetchall()
        print("First 5 top user pincode rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, "
                  f"Pincode: {row[4]}, Users: {row[5]}")

    except Exception as e:
        print(f"❌ Top user read error: {e}")
        conn.rollback()


# --------------------------------Map Insurance Data--------------------------------

# def map_insurance_data():
#     path = "data/map/insurance/country/india/state/"

#     if not os.path.exists(path):
#         print("Path not found!")
#         return None

#     Agg_state_list = os.listdir(path)
#     print(f"Found {len(Agg_state_list)} states for map insurance data")

#     # Data for map insurance - based on JSON format: [lat, lng, metric, label]
#     map_insurance_data = {'State':[], 'Year':[], 'Quarter':[], 'Latitude':[], 'Longitude':[], 'Metric':[], 'Label':[]}

#     for i in Agg_state_list:
#         p_i = path + i + "/"
#         Agg_yr = os.listdir(p_i)

#         for j in Agg_yr:
#             p_j = p_i + j + "/"
#             Agg_yr_list = os.listdir(p_j)

#             for k in Agg_yr_list:
#                 p_k = p_j + k

#                 try:
#                     with open(p_k, 'r') as Data:
#                         D = json.load(Data)

                   
#                     if D['data']['data']:
#                         for location in D['data']['data']:
#                             try:
#                                 if len(location) == 4:
#                                     lat, lng, metric, label = location
                                    
#                                     # Validate data types before adding
#                                     try:
#                                         lat_float = float(lat)
#                                         lng_float = float(lng)
#                                         metric_float = float(metric)
#                                         label_str = str(label)
                                        
#                                         map_insurance_data['State'].append(i)
#                                         map_insurance_data['Year'].append(int(j))
#                                         map_insurance_data['Quarter'].append(int(k.strip('.json')))
#                                         map_insurance_data['Latitude'].append(lat_float)
#                                         map_insurance_data['Longitude'].append(lng_float)
#                                         map_insurance_data['Metric'].append(metric_float)
#                                         map_insurance_data['Label'].append(label_str)
#                                     except (ValueError, TypeError) as val_error:
#                                         print(f"Data validation error in {p_k}: lat={lat}, lng={lng}, metric={metric}, label={label}")
#                                         print(f"Error: {val_error}")
#                                         continue
#                                 else:
#                                     print(f"Skipping location with {len(location)} elements in {p_k}")
#                             except Exception as unpack_error:
#                                 print(f"Error unpacking location {location} in {p_k}: {unpack_error}")
#                                 continue

#                 except Exception as e:
#                     print(f"Error reading {p_k}: {e}")
#                     continue

#     # Create DataFrame
#     Map_Insurance = pd.DataFrame(map_insurance_data)

#     print(f"Successfully created Map Insurance DataFrame with {len(Map_Insurance)} rows")

#     return Map_Insurance

# def create_map_insurance_table(conn):
#     """Create map insurance table in PostgreSQL if it doesn't exist"""
#     try:
#         cursor = conn.cursor()

#         # Check if table already exists and has data
#         cursor.execute("""
#             SELECT EXISTS (
#                 SELECT FROM information_schema.tables 
#                 WHERE table_name = 'map_insurance'
#             )
#         """)
#         table_exists = cursor.fetchone()[0]

#         if table_exists:
#             # Check if table has data
#             cursor.execute("SELECT COUNT(*) FROM map_insurance")
#             row_count = cursor.fetchone()[0]
#             if row_count > 0:
#                 print(f"Table map_insurance already exists with {row_count} rows of data!")
#                 return True
#             else:
#                 print("Table map_insurance exists but is empty. Proceeding with data insertion.")

#         create_map_insurance_query = '''
#             CREATE TABLE IF NOT EXISTS map_insurance (
#                 id SERIAL PRIMARY KEY,
#                 State VARCHAR(100),
#                 Year INT,
#                 Quarter INT,
#                 Latitude NUMERIC(15,12),
#                 Longitude NUMERIC(15,12),
#                 Metric NUMERIC(30,2),
#                 Label VARCHAR(100)
#             )
#         '''

#         cursor.execute(create_map_insurance_query)
#         conn.commit()
#         print("Map insurance table created successfully!")
#         return True

#     except Exception as e:
#         print(f"Map insurance table creation error: {e}")
#         conn.rollback()
#         return False

# def save_map_insurance_to_postgres(df, conn):
#     try:
#         cursor = conn.cursor()

#         # Clear existing data
#         cursor.execute("DELETE FROM map_insurance")
#         conn.commit()

#         # Prepare insert query
#         insert_query = '''
#             INSERT INTO map_insurance (State, Year, Quarter, Latitude, Longitude, Metric, Label)
#             VALUES (%s, %s, %s, %s, %s, %s, %s)
#         '''

#         data = []
#         for idx, row in df.iterrows():
#             try:
#                 data_row = (
#                     row['State'],
#                     int(row['Year']),
#                     int(row['Quarter']),
#                     float(row['Latitude']),
#                     float(row['Longitude']),
#                     float(row['Metric']),
#                     str(row['Label'])
#                 )
#                 data.append(data_row)
#             except (ValueError, TypeError) as val_error:
#                 print(f"Error converting row {idx}: {row}")
#                 print(f"Error: {val_error}")
#                 continue

#         execute_batch(cursor, insert_query, data, page_size=1000)
#         conn.commit()
#         print("Map insurance data saved to PostgreSQL database!")
#         return True

#     except Exception as e:
#         print(f"Map insurance save error: {e}")
#         conn.rollback()
#         return False

# def show_map_insurance_from_postgres(conn):
#     try:
#         cursor = conn.cursor()

#         cursor.execute("SELECT COUNT(*) FROM map_insurance")
#         count = cursor.fetchone()[0]
#         print(f"Total map insurance rows in database: {count}")

#         cursor.execute("SELECT * FROM map_insurance LIMIT 5")
#         rows = cursor.fetchall()

#         print("First 5 map insurance rows:")
#         for row in rows:
#             print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, "
#                   f"Lat: {row[4]}, Lng: {row[5]}, Metric: {row[6]}, Label: {row[7]}")

#     except Exception as e:
#         print(f"Map insurance read error: {e}")
#         conn.rollback()


# --------------------------------Map Insurance Hover Data--------------------------------

def map_insurance_hover_data():
    path = "data/map/insurance/hover/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for map insurance hover data")

    # Data for map insurance hover
    map_insurance_data = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Count':[], 'Amount':[]}

    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = p_j + k

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    # Extract hover data list
                    if D['data']['hoverDataList']:
                        for district in D['data']['hoverDataList']:
                            district_name = district['name']
                            count = district['metric'][0]['count']
                            amount = district['metric'][0]['amount']

                            map_insurance_data['State'].append(i)
                            map_insurance_data['Year'].append(j)
                            map_insurance_data['Quarter'].append(int(k.strip('.json')))
                            map_insurance_data['District'].append(district_name)
                            map_insurance_data['Count'].append(count)
                            map_insurance_data['Amount'].append(amount)

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrame
    Map_Insurance_Hover = pd.DataFrame(map_insurance_data)

    print(f"Successfully created Map Insurance Hover DataFrame with {len(Map_Insurance_Hover)} rows")

    return Map_Insurance_Hover

def create_map_insurance_hover_table(conn):
    """Create map insurance hover table in PostgreSQL if it doesn't exist"""
    try:
        cursor = conn.cursor()

        # Check if table already exists and has data
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'map_insurance_hover'
            )
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            # Check if table has data
            cursor.execute("SELECT COUNT(*) FROM map_insurance_hover")
            row_count = cursor.fetchone()[0]
            if row_count > 0:
                print(f"Table map_insurance_hover already exists with {row_count} rows of data!")
                return True
            else:
                print("Table map_insurance_hover exists but is empty. Proceeding with data insertion.")

        create_map_insurance_query = '''
            CREATE TABLE IF NOT EXISTS map_insurance_hover (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                District VARCHAR(100),
                Count BIGINT,
                Amount NUMERIC(30,2)
            )
        '''

        cursor.execute(create_map_insurance_query)
        conn.commit()
        print("Map insurance hover table created successfully!")
        return True

    except Exception as e:
        print(f"Map insurance hover table creation error: {e}")
        conn.rollback()
        return False

def save_map_insurance_hover_to_postgres(df, conn):
    try:
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("DELETE FROM map_insurance_hover")
        conn.commit()

        # Prepare insert query
        insert_query = '''
            INSERT INTO map_insurance_hover (State, Year, Quarter, District, Count, Amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''

        data = [
            (
                row['State'],
                int(row['Year']),
                int(row['Quarter']),
                row['District'],
                int(row['Count']),
                float(row['Amount'])
            )
            for _, row in df.iterrows()
        ]

        execute_batch(cursor, insert_query, data, page_size=1000)
        conn.commit()
        print("Map insurance hover data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"Map insurance hover save error: {e}")
        conn.rollback()
        return False

def show_map_insurance_hover_from_postgres(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM map_insurance_hover")
        count = cursor.fetchone()[0]
        print(f"Total map insurance hover rows in database: {count}")

        cursor.execute("SELECT * FROM map_insurance_hover LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 map insurance hover rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, District: {row[4]}, Count: {row[5]}, Amount: {row[6]}")

    except Exception as e:
        print(f"Map insurance hover read error: {e}")
        conn.rollback()


# --------------------------------Map transaction hover Data--------------------------------

def map_transaction_hover_data():
    path = "data/map/transaction/hover/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for map transaction hover data")

    # Data for map transaction hover
    map_transaction_data = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Count':[], 'Amount':[]}

    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = p_j + k

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    # Extract hover data list
                    if D['data']['hoverDataList']:
                        for district in D['data']['hoverDataList']:
                            district_name = district['name']
                            count = district['metric'][0]['count']
                            amount = district['metric'][0]['amount']

                            map_transaction_data['State'].append(i)
                            map_transaction_data['Year'].append(j)
                            map_transaction_data['Quarter'].append(int(k.strip('.json')))
                            map_transaction_data['District'].append(district_name)
                            map_transaction_data['Count'].append(count)
                            map_transaction_data['Amount'].append(amount)

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrame
    Map_Transaction_Hover = pd.DataFrame(map_transaction_data)

    print(f"Successfully created Map Transaction Hover DataFrame with {len(Map_Transaction_Hover)} rows")

    return Map_Transaction_Hover

def create_map_transaction_hover_table(conn):
    """Create map transaction hover table in PostgreSQL if it doesn't exist"""
    try:
        cursor = conn.cursor()

        create_map_transaction_query = '''
            CREATE TABLE IF NOT EXISTS map_transaction_hover (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                District VARCHAR(100),
                Count BIGINT,
                Amount NUMERIC(30,2)
            )
        '''

        cursor.execute(create_map_transaction_query)
        conn.commit()
        print("Map transaction hover table created successfully!")
        return True

    except Exception as e:
        print(f"Map transaction hover table creation error: {e}")
        conn.rollback()
        return False

def save_map_transaction_hover_to_postgres(df, conn):
    try:
        cursor = conn.cursor()

        # Clear old data
        cursor.execute("DELETE FROM map_transaction_hover")
        conn.commit()

        insert_query = '''
            INSERT INTO map_transaction_hover (State, Year, Quarter, District, Count, Amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''

        data = [
            (
                row['State'],
                int(row['Year']),
                int(row['Quarter']),
                str(row['District']).title(),
                int(row['Count']),
                float(row['Amount'])
            )
            for _, row in df.iterrows()
        ]

        execute_batch(cursor, insert_query, data, page_size=1000)
        conn.commit()
        print("Map transaction hover data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"Map transaction hover save error: {e}")
        conn.rollback()
        return False

def show_map_transaction_hover_from_postgres(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM map_transaction_hover")
        count = cursor.fetchone()[0]
        print(f"Total map_transaction hover rows in database: {count}")

        cursor.execute("SELECT * FROM map_transaction_hover LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 map transaction hover rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, District: {row[4]}, Count: {row[5]}, Amount: {row[6]}")

    except Exception as e:
        print(f"Map transaction hover read error: {e}")
        conn.rollback()


# --------------------------------Map User hover Data--------------------------------

def map_user_hover_data():
    path = "data/map/user/hover/country/india/state/"

    if not os.path.exists(path):
        print("Path not found!")
        return None

    Agg_state_list = os.listdir(path)
    print(f"Found {len(Agg_state_list)} states for map user hover data")

    # Data for map user hover
    map_user_data = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Registered_Users':[], 'App_Opens':[]}

    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = p_j + k

                try:
                    with open(p_k, 'r') as Data:
                        D = json.load(Data)

                    # Extract hover data (different structure from insurance/transaction)
                    if D['data']['hoverData']:
                        for district_name, district_data in D['data']['hoverData'].items():
                            registered_users = district_data['registeredUsers']
                            app_opens = district_data['appOpens']

                            map_user_data['State'].append(i)
                            map_user_data['Year'].append(j)
                            map_user_data['Quarter'].append(int(k.strip('.json')))
                            map_user_data['District'].append(district_name)
                            map_user_data['Registered_Users'].append(registered_users)
                            map_user_data['App_Opens'].append(app_opens)

                except Exception as e:
                    print(f"Error reading {p_k}: {e}")
                    continue

    # Create DataFrame
    Map_User_Hover = pd.DataFrame(map_user_data)

    print(f"Successfully created Map User Hover DataFrame with {len(Map_User_Hover)} rows")

    return Map_User_Hover

def create_map_user_hover_table(conn):
    """Create map user hover table in PostgreSQL if it doesn't exist"""
    try:
        cursor = conn.cursor()

        create_map_user_query = '''
            CREATE TABLE IF NOT EXISTS map_user_hover (
                id SERIAL PRIMARY KEY,
                State VARCHAR(100),
                Year INT,
                Quarter INT,
                District VARCHAR(100),
                Registered_Users BIGINT,
                App_Opens BIGINT
            )
        '''

        cursor.execute(create_map_user_query)
        conn.commit()
        print("Map user hover table created successfully!")
        return True

    except Exception as e:
        print(f"Map user hover table creation error: {e}")
        conn.rollback()
        return False

def save_map_user_hover_to_postgres(df, conn):
    try:
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("DELETE FROM map_user_hover")
        conn.commit()

        insert_query = '''
            INSERT INTO map_user_hover (State, Year, Quarter, District, Registered_Users, App_Opens)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''

        data = [
            (
                row['State'],
                int(row['Year']),
                int(row['Quarter']),
                str(row['District']).title(),
                int(row['Registered_Users']),
                int(row['App_Opens'])
            )
            for _, row in df.iterrows()
        ]

        execute_batch(cursor, insert_query, data, page_size=1000)
        conn.commit()
        print("Map user hover data saved to PostgreSQL database!")
        return True

    except Exception as e:
        print(f"Map user hover save error: {e}")
        conn.rollback()
        return False

def show_map_user_hover_from_postgres(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM map_user_hover")
        count = cursor.fetchone()[0]
        print(f"Total map user hover rows in database: {count}")

        cursor.execute("SELECT * FROM map_user_hover LIMIT 5")
        rows = cursor.fetchall()

        print("First 5 map user hover rows:")
        for row in rows:
            print(f"ID: {row[0]}, State: {row[1]}, Year: {row[2]}, Quarter: {row[3]}, District: {row[4]}, Users: {row[5]}, App Opens: {row[6]}")

    except Exception as e:
        print(f"Map user hover read error: {e}")
        conn.rollback()


# --------------------------------Main Function--------------------------------
def main():
    # Get PostgreSQL connection
    conn = connect_to_database()
    if conn is None:
        print("Failed to connect to PostgreSQL!")
        return

#--------------------------------Aggregated Insurance Data--------------------------------
    # Check if insurance data already exists
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM agg_insurance")
    insurance_count = cursor.fetchone()[0]
    
    if insurance_count == 0:
        # Extract insurance data
        insurance_df = agg_insurance_data()
        if insurance_df is None:
            print("Failed to extract insurance data!")
            conn.close()
            return
        
        # Create insurance table
        if not insurance_table(conn):
            print("Failed to create insurance table!")
            conn.close()
            return
        
        # Save insurance data
        insurance_success = save_to_postgres(insurance_df, conn)
        if not insurance_success:
            print("Failed to save insurance data!")
            conn.close()
            return
        
        show_data_from_postgres(conn)
    else:
        print(f"Insurance data already exists ({insurance_count} rows), skipping...")

    #--------------------------------Aggregated Transaction Data--------------------------------
    # Check if transaction data already exists
    try:
        cursor.execute("SELECT COUNT(*) FROM agg_transaction")
        transaction_count = cursor.fetchone()[0]
        
        if transaction_count == 0:
            # Extract transaction data
            transaction_df = agg_transaction_data()
            if transaction_df is None:
                print("Failed to extract transaction data!")
                conn.close()
                return
            
            # Create transaction table
            if not transaction_table(conn):
                print("Failed to create transaction table!")
                conn.close()
                return
            
            # Save transaction data
            transaction_success = agg_transaction_db_save(transaction_df, conn)
            if not transaction_success:
                print("Failed to save transaction data!")
                conn.close()
                return
            
            agg_transaction_db_show(conn)
        else:
            print(f"Transaction data already exists ({transaction_count} rows), skipping...")
    except Exception as e:
        print("Transaction table doesn't exist, creating and processing... " + str(e))
        
        # Extract transaction data
        transaction_df = agg_transaction_data()
        if transaction_df is None:
            print("Failed to extract transaction data!")
            conn.close()
            return
        
        # Create transaction table
        if not transaction_table(conn):
            print("Failed to create transaction table!")
            conn.close()
            return
        
        # Save transaction data
        transaction_success = agg_transaction_db_save(transaction_df, conn)
        if not transaction_success:
            print("Failed to save transaction data!")
            conn.close()
            return
        
        agg_transaction_db_show(conn)
        
    #--------------------------------Aggregated User Data--------------------------------
    # Check if user data already exists
    try:
        cursor.execute("SELECT COUNT(*) FROM agg_user")
        user_count = cursor.fetchone()[0]
        
        if user_count == 0:
            # Extract user data
            user_aggregated_df, user_device_df = agg_user_data()
            if user_aggregated_df is None or user_device_df is None:
                print("Failed to extract user data!")
                conn.close()
                return

            # Create user tables
            if not create_user_tables(conn):
                print("Failed to create user tables!")
                conn.close()
                return

            # Save user data
            user_success = save_user_data_to_postgres(user_aggregated_df, user_device_df, conn)
            if not user_success:
                print("Failed to save user data!")
                conn.close()
                return

            show_user_data_from_postgres(conn)
        else:
            print(f"User data already exists ({user_count} rows), skipping...")
    except Exception as e:
        print("User table doesn't exist, creating and processing..." + str(e))
        
        # Extract user data
        user_aggregated_df, user_device_df = agg_user_data()
        if user_aggregated_df is None or user_device_df is None:
            print("Failed to extract user data!")
            conn.close()
            return

        # Create user tables
        if not create_user_tables(conn):
            print("Failed to create user tables!")
            conn.close()
            return

        # Save user data
        user_success = save_user_data_to_postgres(user_aggregated_df, user_device_df, conn)
        if not user_success:
            print("Failed to save user data!")
            conn.close()
            return

        show_user_data_from_postgres(conn)

    # -------------- Top Insurance Data ---------------------------------
    conn.rollback()
    
    # Get a fresh cursor after rollback
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM top_insurance_district")
        top_insurance_district_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM top_insurance_pincode")
        top_insurance_pincode_count = cursor.fetchone()[0]
        
        if top_insurance_district_count == 0 and top_insurance_pincode_count == 0:
            print("Top insurance data doesn't exist, creating and processing...")
            # Extract top insurance data
            top_insurance_district_df, top_insurance_pincode_df = top_insurance_data()
            if top_insurance_district_df is None or top_insurance_pincode_df is None:
                print("Failed to extract top insurance data!")
                conn.close()
                return

            # Create top insurance tables
            if not create_top_insurance_tables(conn):
                print("Failed to create top insurance tables!")
                conn.close()
                return

            # Save top insurance data
            top_insurance_success = save_top_insurance_to_postgres(top_insurance_district_df, top_insurance_pincode_df, conn)
            if not top_insurance_success:
                print("Failed to save top insurance data!")
                conn.close()
                return

            show_top_insurance_from_postgres(conn)
        else:
            print(f"Top insurance data already exists (District: {top_insurance_district_count} rows, Pincode: {top_insurance_pincode_count} rows), skipping...")
    except Exception as e:
        # Tables don't exist, so process top insurance data
        print("Top insurance tables don't exist, creating and processing... " + str(e))
        
        # Extract top insurance data
        top_insurance_district_df, top_insurance_pincode_df = top_insurance_data()
        if top_insurance_district_df is None or top_insurance_pincode_df is None:
            print("Failed to extract top insurance data!")
            conn.close()
            return

        # Create top insurance tables
        if not create_top_insurance_tables(conn):
            print("Failed to create top insurance tables!")
            conn.close()
            return

        # Save top insurance data
        top_insurance_success = save_top_insurance_to_postgres(top_insurance_district_df, top_insurance_pincode_df, conn)
        if not top_insurance_success:
            print("Failed to save top insurance data!")
            conn.close()
            return

        show_top_insurance_from_postgres(conn)

    # -------------- Top Transaction Data ---------------------------------
    conn.rollback()
    
    # Get a fresh cursor after rollback
    cursor = conn.cursor()
    
    # Check if top transaction data already exists
    try:
        cursor.execute("SELECT COUNT(*) FROM top_transaction_district")
        top_transaction_district_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM top_transaction_pincode")
        top_transaction_pincode_count = cursor.fetchone()[0]
        
        if top_transaction_district_count == 0 and top_transaction_pincode_count == 0:
            print("Top transaction data doesn't exist, creating and processing...")
            # Extract top transaction data
            top_transaction_district_df, top_transaction_pincode_df = top_transaction_data()
            if top_transaction_district_df is None or top_transaction_pincode_df is None:
                print("Failed to extract top transaction data!")
                conn.close()
                return

            # Create top transaction tables
            if not create_top_transaction_tables(conn):
                print("Failed to create top transaction tables!")
                conn.close()
                return

            # Save top transaction data
            top_transaction_success = save_top_transaction_to_postgres(top_transaction_district_df, top_transaction_pincode_df, conn)
            if not top_transaction_success:
                print("Failed to save top transaction data!")
                conn.close()
                return

            show_top_transaction_from_postgres(conn)
        else:
            print(f"Top transaction data already exists (District: {top_transaction_district_count} rows, Pincode: {top_transaction_pincode_count} rows), skipping...")
    except Exception as e:
        # Tables don't exist, so process top transaction data
        print("Top transaction tables don't exist, creating and processing... " + str(e))
        
        # Extract top transaction data
        top_transaction_district_df, top_transaction_pincode_df = top_transaction_data()
        if top_transaction_district_df is None or top_transaction_pincode_df is None:
            print("Failed to extract top transaction data!")
            conn.close()
            return

        # Create top transaction tables
        if not create_top_transaction_tables(conn):
            print("Failed to create top transaction tables!")
            conn.close()
            return

        # Save top transaction data
        top_transaction_success = save_top_transaction_to_postgres(top_transaction_district_df, top_transaction_pincode_df, conn)
        if not top_transaction_success:
            print("Failed to save top transaction data!")
            conn.close()
            return

        show_top_transaction_from_postgres(conn)

    # -------------- Top User Data ---------------------------------
    conn.rollback()
    
    # Get a fresh cursor after rollback
    cursor = conn.cursor()
    
    # Check if top user data already exists
    try:
        cursor.execute("SELECT COUNT(*) FROM top_user_district")
        top_user_district_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM top_user_pincode")
        top_user_pincode_count = cursor.fetchone()[0]
        
        if top_user_district_count == 0 and top_user_pincode_count == 0:
            print("Top user data doesn't exist, creating and processing...")
            # Extract top user data
            top_user_district_df, top_user_pincode_df = top_user_data()
            if top_user_district_df is None or top_user_pincode_df is None:
                print("Failed to extract top user data!")
                conn.close()
                return

            # Create top user tables
            if not create_top_user_tables(conn):
                print("Failed to create top user tables!")
                conn.close()
                return

            # Save top user data
            top_user_success = save_top_user_to_postgres(top_user_district_df, top_user_pincode_df, conn)
            if not top_user_success:
                print("Failed to save top user data!")
                conn.close()
                return

            show_top_user_from_postgres(conn)
        else:
            print(f"Top user data already exists (District: {top_user_district_count} rows, Pincode: {top_user_pincode_count} rows), skipping...")
    except Exception as e:
        # Tables don't exist, so process top user data
        print("Top user tables don't exist, creating and processing..." + str(e))
        
        # Extract top user data
        top_user_district_df, top_user_pincode_df = top_user_data()
        if top_user_district_df is None or top_user_pincode_df is None:
            print("Failed to extract top user data!")
            conn.close()
            return

        # Create top user tables
        if not create_top_user_tables(conn):
            print("Failed to create top user tables!")
            conn.close()
            return

        # Save top user data
        top_user_success = save_top_user_to_postgres(top_user_district_df, top_user_pincode_df, conn)
        if not top_user_success:
            print("Failed to save top user data!")
            conn.close()
            return

        show_top_user_from_postgres(conn)
        
    # -------------- Map Insurance Data ---------------------------------
    
    # try:
    #     # Get a fresh cursor after rollback
    #     cursor = conn.cursor()
        
    #     # Check if map insurance data already exists
    #     cursor.execute("SELECT COUNT(*) FROM map_insurance")
    #     map_insurance_count = cursor.fetchone()[0]
        
    #     if map_insurance_count == 0:
    #         print("Map insurance data doesn't exist, creating and processing...")
    #         # Extract map insurance data
    #         map_insurance_df = map_insurance_data()
    #         if map_insurance_df is None:
    #             print("Failed to extract map insurance data!")
    #             conn.rollback()
    #             conn.close()
    #             return

    #         # Create map insurance table
    #         if not create_map_insurance_table(conn):
    #             print("Failed to create map insurance table!")
    #             conn.rollback()
    #             conn.close()
    #             return

    #         # Save map insurance data
    #         map_insurance_success = save_map_insurance_to_postgres(map_insurance_df, conn)
    #         if not map_insurance_success:
    #             print("Failed to save map insurance data!")
    #             conn.rollback()
    #             conn.close()
    #             return

    #         show_map_insurance_from_postgres(conn)
    #     else:
    #         print(f"Map insurance data already exists with {map_insurance_count} rows, skipping...")
            
    # except Exception as e:
    #     # Table doesn't exist, so process map insurance data
    #     print("Map insurance table doesn't exist, creating and processing..." + str(e))
    #     conn.rollback()
        
    #     try:
    #         # Extract map insurance data
    #         map_insurance_df = map_insurance_data()
    #         if map_insurance_df is None:
    #             print("Failed to extract map insurance data!")
    #             conn.rollback()
    #             conn.close()
    #             return

    #         # Create map insurance table
    #         if not create_map_insurance_table(conn):
    #             print("Failed to create map insurance table!")
    #             conn.rollback()
    #             conn.close()
    #             return

    #         # Save map insurance data
    #         map_insurance_success = save_map_insurance_to_postgres(map_insurance_df, conn)
    #         if not map_insurance_success:
    #             print("Failed to save map insurance data!")
    #             conn.rollback()
    #             conn.close()
    #             return

    #         show_map_insurance_from_postgres(conn)
            
    #     except Exception as inner_e:
    #         print(f"Error during map insurance data processing: {inner_e}")
    #         conn.rollback()
    #         conn.close()
    #         return
        
    # -------------- Map Insurance Hover Data ---------------------------------
    
    try:
        # Get a fresh cursor after rollback
        cursor = conn.cursor()
        
        # Check if map insurance hover data already exists
        cursor.execute("SELECT COUNT(*) FROM map_insurance_hover")
        map_insurance_count = cursor.fetchone()[0]
        
        if map_insurance_count == 0:
            print("Map insurance hover data doesn't exist, creating and processing...")
            # Extract map insurance hover data
            map_insurance_hover_df = map_insurance_hover_data()
            if map_insurance_hover_df is None:
                print("Failed to extract map insurance hover data!")
                conn.rollback()
                conn.close()
                return

            # Create map insurance hover table
            if not create_map_insurance_hover_table(conn):
                print("Failed to create map insurance hover table!")
                conn.rollback()
                conn.close()
                return

            # Save map insurance hover data
            map_insurance_success = save_map_insurance_hover_to_postgres(map_insurance_hover_df, conn)
            if not map_insurance_success:
                print("Failed to save map insurance hover data!")
                conn.rollback()
                conn.close()
                return

            show_map_insurance_hover_from_postgres(conn)
        else:
            print(f"Map insurance hover data already exists with {map_insurance_count} rows, skipping...")
            
    except Exception as e:
        # Table doesn't exist, so process map insurance hover data
        print("Map insurance hover table doesn't exist, creating and processing..." + str(e))
        conn.rollback()
        
        try:
            # Extract map insurance hover data
            map_insurance_hover_df = map_insurance_hover_data()
            if map_insurance_hover_df is None:
                print("Failed to extract map insurance hover data!")
                conn.rollback()
                conn.close()
                return

            # Create map insurance hover table
            if not create_map_insurance_hover_table(conn):
                print("Failed to create map insurance hover table!")
                conn.rollback()
                conn.close()
                return

            # Save map insurance hover data
            map_insurance_success = save_map_insurance_hover_to_postgres(map_insurance_hover_df, conn)
            if not map_insurance_success:
                print("Failed to save map insurance hover data!")
                conn.rollback()
                conn.close()
                return

            show_map_insurance_hover_from_postgres(conn)
            
        except Exception as inner_e:
            print(f"Error during map insurance hover data processing: {inner_e}")
            conn.rollback()
            conn.close()
            return

    # -------------- Map Transaction Hover Data ---------------------------------
    
    try:
        # Get a fresh cursor after rollback
        cursor = conn.cursor()
        
        # Check if map transaction hover data already exists
        cursor.execute("SELECT COUNT(*) FROM map_transaction_hover")
        map_transaction_count = cursor.fetchone()[0]
        
        if map_transaction_count == 0:
            print("Map transaction hover data doesn't exist, creating and processing...")
            # Extract map transaction hover data
            map_transaction_hover_df = map_transaction_hover_data()
            if map_transaction_hover_df is None:
                print("Failed to extract map transaction hover data!")
                conn.rollback()
                conn.close()
                return

            # Create map transaction hover table
            if not create_map_transaction_hover_table(conn):
                print("Failed to create map transaction hover table!")
                conn.rollback()
                conn.close()
                return

            # Save map transaction hover data
            map_transaction_success = save_map_transaction_hover_to_postgres(map_transaction_hover_df, conn)
            if not map_transaction_success:
                print("Failed to save map transaction hover data!")
                conn.rollback()
                conn.close()
                return

            show_map_transaction_hover_from_postgres(conn)
        else:
            print(f"Map transaction hover data already exists with {map_transaction_count} rows, skipping...")
            
    except Exception as e:
        # Table doesn't exist, so process map transaction hover data
        print("Map transaction hover table doesn't exist, creating and processing..." + str(e))
        conn.rollback()
        
        try:
            # Extract map transaction hover data
            map_transaction_hover_df = map_transaction_hover_data()
            if map_transaction_hover_df is None:
                print("Failed to extract map transaction hover data!")
                conn.rollback()
                conn.close()
                return

            # Create map transaction hover table
            if not create_map_transaction_hover_table(conn):
                print("Failed to create map transaction hover table!")
                conn.rollback()
                conn.close()
                return

            # Save map transaction hover data
            map_transaction_success = save_map_transaction_hover_to_postgres(map_transaction_hover_df, conn)
            if not map_transaction_success:
                print("Failed to save map transaction hover data!")
                conn.rollback()
                conn.close()
                return

            show_map_transaction_hover_from_postgres(conn)
            
        except Exception as inner_e:
            print(f"Error during map transaction hover data processing: {inner_e}")
            conn.rollback()
            conn.close()
            return

    # -------------- Map User Hover Data ---------------------------------
    
    try:
        # Get a fresh cursor after rollback
        cursor = conn.cursor()
        
        # Check if map user hover data already exists
        cursor.execute("SELECT COUNT(*) FROM map_user_hover")
        map_user_count = cursor.fetchone()[0]
        
        if map_user_count == 0:
            print("Map user hover data doesn't exist, creating and processing...")
            # Extract map user hover data
            map_user_hover_df = map_user_hover_data()
            if map_user_hover_df is None:
                print("Failed to extract map user hover data!")
                conn.rollback()
                conn.close()
                return

            # Create map user hover table
            if not create_map_user_hover_table(conn):
                print("Failed to create map user hover table!")
                conn.rollback()
                conn.close()
                return

            # Save map user hover data
            map_user_success = save_map_user_hover_to_postgres(map_user_hover_df, conn)
            if not map_user_success:
                print("Failed to save map user hover data!")
                conn.rollback()
                conn.close()
                return

            show_map_user_hover_from_postgres(conn)
        else:
            print(f"Map user hover data already exists with {map_user_count} rows, skipping...")
            
    except Exception as e:
        print("Map user hover table doesn't exist, creating and processing..." + str(e))
        conn.rollback()
        
        try:
            # Extract map user hover data
            map_user_hover_df = map_user_hover_data()
            if map_user_hover_df is None:
                print("Failed to extract map user hover data!")
                conn.rollback()
                conn.close()
                return

            # Create map user hover table
            if not create_map_user_hover_table(conn):
                print("Failed to create map user hover table!")
                conn.rollback()
                conn.close()
                return

            # Save map user hover data
            map_user_success = save_map_user_hover_to_postgres(map_user_hover_df, conn)
            if not map_user_success:
                print("Failed to save map user hover data!")
                conn.rollback()
                conn.close()
                return

            show_map_user_hover_from_postgres(conn)
            
        except Exception as inner_e:
            print(f"Error during map user hover data processing: {inner_e}")
            conn.rollback()
            conn.close()
            return

    conn.close()
    print("\n=== Done! ===")


if __name__ == "__main__":
    main()
