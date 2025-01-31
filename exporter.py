import oracledb
import csv

import env

USERNAME = "STRENKOWSKIP"
PASSWORD = env.PASSWORD
DSN = "213.184.8.44:1521/orcl"

def export_table_to_csv(table_name, csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    cursor.execute(f"SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER(:1) ORDER BY COLUMN_ID", [table_name])
    columns = [row[0] for row in cursor.fetchall()]

    select_sql = f"SELECT * FROM {table_name}"
    cursor.execute(select_sql)

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(columns)
        
        for row in cursor:
            writer.writerow(row)

    cursor.close()
    connection.close()
    print(f"Wyeksportowano tabelę {table_name} do pliku {csv_file_path}.")

if __name__ == "__main__":
    tables = [
        "UŻYTKOWNIK",
        "ULGA",
        "PRODUKT",
        "PRODUKT_TRANSAKCJA",
        "TRANSAKCJA",
        "BILET",
        "FILM",
        "SEANS",
        "SALA",
        "PLACÓWKA",
        "ROLA",
    ]

    for tbl in tables:
        csv_name = f"data/{tbl.lower()}.csv"
        export_table_to_csv(tbl, csv_name)
