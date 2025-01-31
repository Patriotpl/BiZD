import oracledb

import env

USERNAME = "STRENKOWSKIP"
PASSWORD = env.PASSWORD
DSN = "213.184.8.44:1521/orcl"

def delete_table_data(table_name):
    try:
        connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
        cursor = connection.cursor()
        query = f"DELETE FROM {table_name}"
        cursor.execute(query)
        connection.commit()
        print(f"Dane zostały usunięte z tabeli {table_name}.")
    except oracledb.DatabaseError as e:
        print(f"Błąd bazy danych: {e}")
    finally:
        cursor.close()
        connection.close()

for table in [
    "produkt_transakcja",
    "produkt",
    "transakcja",
    "użytkownik"
    "rola",
    "ulga",
    "bilet",
    "seans",
    "film",
    "sala",
    "placówka"
]:
    delete_table_data(table.upper())