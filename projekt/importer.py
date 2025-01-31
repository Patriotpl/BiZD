import oracledb
import csv

import env

USERNAME = "STRENKOWSKIP"
PASSWORD = env.PASSWORD
DSN = "213.184.8.44:1521/orcl"  

def row_exists(cursor, table_name, pk_column, pk_value):
    query = f"SELECT COUNT(*) FROM {table_name} WHERE {pk_column} = :val"
    cursor.execute(query, val=pk_value)
    (count,) = cursor.fetchone()
    return count > 0

def load_uzytkownik(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_UŻYTKOWNIKA"]:
                print("Pominięto wiersz bez klucza głównego (ID_UŻYTKOWNIKA).")
                continue

            id_u = int(row["ID_UŻYTKOWNIKA"])

            if row_exists(cursor, "UŻYTKOWNIK", "ID_UŻYTKOWNIKA", id_u):
                print(f"Wiersz o ID_UŻYTKOWNIKA={id_u} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO UŻYTKOWNIK (
                    ID_UŻYTKOWNIKA,
                    NAZWISKO,
                    IMIĘ,
                    LOGIN,
                    ID_ROLI,
                    ID_ULGI,
                    ADRES_ZAMIESZKANIA,
                    PREMIA,
                    HASŁO,
                    ID_PLACÓWKI
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10
                )
            """

            nazwisko = row["NAZWISKO"].strip() if row["NAZWISKO"] else None
            imie = row["IMIĘ"].strip() if row["IMIĘ"] else None
            login = row["LOGIN"].strip() if row["LOGIN"] else None
            id_roli = int(row["ID_ROLI"]) if row["ID_ROLI"] else None
            id_ulgi = int(row["ID_ULGI"]) if row["ID_ULGI"] else None
            adres = row["ADRES_ZAMIESZKANIA"].strip() if row["ADRES_ZAMIESZKANIA"] else None
            premia = float(row["PREMIA"]) if row["PREMIA"] else 0
            haslo = row["HASŁO"].strip() if row["HASŁO"] else None
            id_placowki = int(row["ID_PLACÓWKI"]) if row["ID_PLACÓWKI"] else None

            cursor.execute(insert_sql, (
                id_u,
                nazwisko,
                imie,
                login,
                id_roli,
                id_ulgi,
                adres,
                premia,
                haslo,
                id_placowki
            ))
            print(f"Dodano wiersz ID_UŻYTKOWNIKA={id_u}")

    connection.commit()
    cursor.close()
    connection.close()

def load_archive(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_WPISU"]:
                print("Pominięto wiersz bez klucza głównego (ID_WPISU).")
                continue

            id_wpisu = int(row["ID_WPISU"])
            tabela = row["TABELA"].strip() if row["TABELA"] else None
            operacja = row["OPERACJA"].strip() if row["OPERACJA"] else None
            uzytkownik = row["UZYTKOWNIK"].strip() if row["UZYTKOWNIK"] else None
            blad = row["BLAD"].strip() if row["BLAD"] else None
            data = row["DATA"].strip() if row["DATA"] else None

            if row_exists(cursor, "STRENKOWSKIP.ARCHIWUM", "ID_WPISU", id_wpisu):
                print(f"Wiersz o ID_WPISU={id_wpisu} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.ARCHIWUM (
                    ID_WPISU, TABELA, OPERACJA, UZYTKOWNIK, BLAD, "DATA"
                ) VALUES (
                    :1, :2, :3, :4, :5, TO_DATE(:6, 'YYYY-MM-DD HH24:MI:SS')
                )
            """

            cursor.execute(insert_sql, (
                id_wpisu,
                tabela,
                operacja,
                uzytkownik,
                blad,
                data
            ))
            print(f"Dodano wiersz ID_WPISU={id_wpisu}")

    connection.commit()
    cursor.close()
    connection.close()

def load_ticket(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_BILETU"]:
                print("Pominięto wiersz bez klucza głównego (ID_BILETU).")
                continue

            id_biletu = int(row["ID_BILETU"])
            id_seansu = int(row["ID_SEANSU"]) if row["ID_SEANSU"] else None

            if id_seansu and not row_exists(cursor, "STRENKOWSKIP.SEANS", "ID_SEANSU", id_seansu):
                print(f"ID_SEANSU={id_seansu} nie istnieje w tabeli SEANS - pomijam.")
                continue

            if row_exists(cursor, "STRENKOWSKIP.BILET", "ID_BILETU", id_biletu):
                print(f"Wiersz o ID_BILETU={id_biletu} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.BILET (
                    ID_BILETU, ID_SEANSU
                ) VALUES (
                    :1, :2
                )
            """

            cursor.execute(insert_sql, (id_biletu, id_seansu))
            print(f"Dodano wiersz ID_BILETU={id_biletu}")

    connection.commit()
    cursor.close()
    connection.close()

def load_movie(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_FILMU"]:
                print("Pominięto wiersz bez klucza głównego (ID_FILMU).")
                continue

            id_filmu = int(row["ID_FILMU"])
            tytul = row["TYTUŁ"].strip() if row["TYTUŁ"] else None
            dlugosc = int(row["DŁUGOŚĆ"]) if row["DŁUGOŚĆ"] else None
            wydawca = row["WYDAWCA"].strip() if row["WYDAWCA"] else None
            reżyser = row["REŻYSER"].strip() if row["REŻYSER"] else None
            gatunek = row["GATUNEK"].strip() if row["GATUNEK"] else None

            if not all([id_filmu, tytul, dlugosc, wydawca, reżyser, gatunek]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_FILMU={id_filmu}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.FILM", "ID_FILMU", id_filmu):
                print(f"Wiersz o ID_FILMU={id_filmu} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.FILM (
                    ID_FILMU, TYTUŁ, DŁUGOŚĆ, WYDAWCA, REŻYSER, GATUNEK
                ) VALUES (
                    :1, :2, :3, :4, :5, :6
                )
            """

            cursor.execute(insert_sql, (
                id_filmu,
                tytul,
                dlugosc,
                wydawca,
                reżyser,
                gatunek
            ))
            print(f"Dodano wiersz ID_FILMU={id_filmu}")

    connection.commit()
    cursor.close()
    connection.close()


def load_place(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_PLACÓWKI"]:
                print("Pominięto wiersz bez klucza głównego (ID_PLACÓWKI).")
                continue

            id_placowki = int(row["ID_PLACÓWKI"])
            adres_placowki = row["ADRES_PLACÓWKI"].strip() if row["ADRES_PLACÓWKI"] else None
            kod_pocztowy = row["KOD_POCZTOWY"].strip() if row["KOD_POCZTOWY"] else None

            if not all([id_placowki, adres_placowki, kod_pocztowy]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_PLACÓWKI={id_placowki}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.PLACÓWKA", "ID_PLACÓWKI", id_placowki):
                print(f"Wiersz o ID_PLACÓWKI={id_placowki} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.PLACÓWKA (
                    ID_PLACÓWKI, ADRES_PLACÓWKI, KOD_POCZTOWY
                ) VALUES (
                    :1, :2, :3
                )
            """

            cursor.execute(insert_sql, (
                id_placowki,
                adres_placowki,
                kod_pocztowy
            ))
            print(f"Dodano wiersz ID_PLACÓWKI={id_placowki}")

    connection.commit()
    cursor.close()
    connection.close()


def load_product_transaction(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_POWIĄZANIA"]:
                print("Pominięto wiersz bez klucza głównego (ID_POWIĄZANIA).")
                continue

            id_powiazania = int(row["ID_POWIĄZANIA"])
            id_produktu = int(row["ID_PRODUKTU"]) if row["ID_PRODUKTU"] else None
            id_transakcji = int(row["ID_TRANSAKCJI"]) if row["ID_TRANSAKCJI"] else None
            ilosc_produktu = int(row["ILOSC_PRODUKTU"]) if row["ILOSC_PRODUKTU"] else 1

            if not all([id_produktu, id_transakcji]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_POWIĄZANIA={id_powiazania}).")
                continue

            if not row_exists(cursor, "STRENKOWSKIP.PRODUKT", "ID_PRODUKTU", id_produktu):
                print(f"ID_PRODUKTU={id_produktu} nie istnieje w tabeli PRODUKT - pomijam.")
                continue
            if not row_exists(cursor, "STRENKOWSKIP.TRANSAKCJA", "ID_TRANSAKCJI", id_transakcji):
                print(f"ID_TRANSAKCJI={id_transakcji} nie istnieje w tabeli TRANSAKCJA - pomijam.")
                continue

            if row_exists(cursor, "STRENKOWSKIP.PRODUKT_TRANSAKCJA", "ID_POWIĄZANIA", id_powiazania):
                print(f"Wiersz o ID_POWIĄZANIA={id_powiazania} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.PRODUKT_TRANSAKCJA (
                    ID_POWIĄZANIA, ID_PRODUKTU, ID_TRANSAKCJI, ILOSC_PRODUKTU
                ) VALUES (
                    :1, :2, :3, :4
                )
            """

            cursor.execute(insert_sql, (
                id_powiazania,
                id_produktu,
                id_transakcji,
                ilosc_produktu
            ))
            print(f"Dodano wiersz ID_POWIĄZANIA={id_powiazania}")

    connection.commit()
    cursor.close()
    connection.close()

def load_product(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_PRODUKTU"]:
                print("Pominięto wiersz bez klucza głównego (ID_PRODUKTU).")
                continue

            id_produktu = int(row["ID_PRODUKTU"])
            nazwa = row["NAZWA"].strip() if row["NAZWA"] else None
            cena = float(row["CENA"]) if row["CENA"] else None

            if not all([id_produktu, nazwa, cena]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_PRODUKTU={id_produktu}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.PRODUKT", "ID_PRODUKTU", id_produktu):
                print(f"Wiersz o ID_PRODUKTU={id_produktu} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.PRODUKT (
                    ID_PRODUKTU, NAZWA, CENA
                ) VALUES (
                    :1, :2, :3
                )
            """

            cursor.execute(insert_sql, (
                id_produktu,
                nazwa,
                cena
            ))
            print(f"Dodano wiersz ID_PRODUKTU={id_produktu}")

    connection.commit()
    cursor.close()
    connection.close()

def load_role(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_ROLI"]:
                print("Pominięto wiersz bez klucza głównego (ID_ROLI).")
                continue

            id_roli = int(row["ID_ROLI"])
            nazwa = row["NAZWA"].strip() if row["NAZWA"] else None
            pensja_bazowa = float(row["PENSJA_BAZOWA"]) if row["PENSJA_BAZOWA"] else 0

            if not all([id_roli, nazwa, pensja_bazowa is not None]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_ROLI={id_roli}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.ROLA", "ID_ROLI", id_roli):
                print(f"Wiersz o ID_ROLI={id_roli} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.ROLA (
                    ID_ROLI, NAZWA, PENSJA_BAZOWA
                ) VALUES (
                    :1, :2, :3
                )
            """

            cursor.execute(insert_sql, (
                id_roli,
                nazwa,
                pensja_bazowa
            ))
            print(f"Dodano wiersz ID_ROLI={id_roli}")

    connection.commit()
    cursor.close()
    connection.close()


def load_venue(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_SALI"]:
                print("Pominięto wiersz bez klucza głównego (ID_SALI).")
                continue

            id_sali = int(row["ID_SALI"])
            ilosc_miejsc = int(row["ILOŚĆ_MIEJSC"]) if row["ILOŚĆ_MIEJSC"] else None
            czy_czynna = int(row["CZY_CZYNNA"]) if row["CZY_CZYNNA"] else 0
            id_placowki = int(row["ID_PLACÓWKI"]) if row["ID_PLACÓWKI"] else 1

            if not all([id_sali, ilosc_miejsc is not None, czy_czynna is not None, id_placowki is not None]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_SALI={id_sali}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.SALA", "ID_SALI", id_sali):
                print(f"Wiersz o ID_SALI={id_sali} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.SALA (
                    ID_SALI, ILOŚĆ_MIEJSC, CZY_CZYNNA, ID_PLACÓWKI
                ) VALUES (
                    :1, :2, :3, :4
                )
            """

            cursor.execute(insert_sql, (
                id_sali,
                ilosc_miejsc,
                czy_czynna,
                id_placowki
            ))
            print(f"Dodano wiersz ID_SALI={id_sali}")

    connection.commit()
    cursor.close()
    connection.close()


def load_seance(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_SEANSU"]:
                print("Pominięto wiersz bez klucza głównego (ID_SEANSU).")
                continue

            id_seansu = int(row["ID_SEANSU"])
            id_sali = int(row["ID_SALI"]) if row["ID_SALI"] else None
            cena_bazowa = float(row["CENA_BAZOWA"]) if row["CENA_BAZOWA"] else 24.99
            poczatek = row["POCZĄTEK"].strip() if row["POCZĄTEK"] else None
            id_filmu = int(row["ID_FILMU"]) if row["ID_FILMU"] else None

            if not all([id_seansu, id_sali, cena_bazowa is not None, poczatek, id_filmu]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_SEANSU={id_seansu}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.SEANS", "ID_SEANSU", id_seansu):
                print(f"Wiersz o ID_SEANSU={id_seansu} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.SEANS (
                    ID_SEANSU, ID_SALI, CENA_BAZOWA, POCZĄTEK, ID_FILMU
                ) VALUES (
                    :1, :2, :3, TO_TIMESTAMP(:4, 'YYYY-MM-DD HH24:MI:SS.FF'), :5
                )
            """

            cursor.execute(insert_sql, (
                id_seansu,
                id_sali,
                cena_bazowa,
                poczatek,
                id_filmu
            ))
            print(f"Dodano wiersz ID_SEANSU={id_seansu}")

    connection.commit()
    cursor.close()
    connection.close()


def load_transaction(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_TRANSAKCJI"]:
                print("Pominięto wiersz bez klucza głównego (ID_TRANSAKCJI).")
                continue

            id_transakcji = int(row["ID_TRANSAKCJI"])
            data = row["DATA"].strip() if row["DATA"] else None
            id_nabywcy = int(row["ID_NABYWCY"]) if row["ID_NABYWCY"] else None
            id_sprzedawcy = int(row["ID_SPRZEDAWCY"]) if row["ID_SPRZEDAWCY"] else None
            bilans = float(row["BILANS"]) if row["BILANS"] else None

            if not all([id_transakcji, data, id_nabywcy, id_sprzedawcy, bilans is not None]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_TRANSAKCJI={id_transakcji}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.TRANSAKCJA", "ID_TRANSAKCJI", id_transakcji):
                print(f"Wiersz o ID_TRANSAKCJI={id_transakcji} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.TRANSAKCJA (
                    ID_TRANSAKCJI, "DATA", ID_NABYWCY, ID_SPRZEDAWCY, BILANS
                ) VALUES (
                    :1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS.FF'), :3, :4, :5
                )
            """

            cursor.execute(insert_sql, (
                id_transakcji,
                data,
                id_nabywcy,
                id_sprzedawcy,
                bilans
            ))
            print(f"Dodano wiersz ID_TRANSAKCJI={id_transakcji}")

    connection.commit()
    cursor.close()
    connection.close()


def load_discount(csv_file_path):
    connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
    cursor = connection.cursor()

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            if not row["ID_ULGI"]:
                print("Pominięto wiersz bez klucza głównego (ID_ULGI).")
                continue

            id_ulgi = int(row["ID_ULGI"])
            nazwa = row["NAZWA"].strip() if row["NAZWA"] else None
            znizka = float(row["ZNIŻKA"]) if row["ZNIŻKA"] else None

            if not all([id_ulgi, nazwa, znizka is not None]):
                print(f"Pominięto wiersz z brakującymi danymi (ID_ULGI={id_ulgi}).")
                continue

            if row_exists(cursor, "STRENKOWSKIP.ULGA", "ID_ULGI", id_ulgi):
                print(f"Wiersz o ID_ULGI={id_ulgi} już istnieje - pomijam.")
                continue

            insert_sql = """
                INSERT INTO STRENKOWSKIP.ULGA (
                    ID_ULGI, NAZWA, ZNIŻKA
                ) VALUES (
                    :1, :2, :3
                )
            """

            cursor.execute(insert_sql, (
                id_ulgi,
                nazwa,
                znizka
            ))
            print(f"Dodano wiersz ID_ULGI={id_ulgi}")

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    load_movie("data/film.csv")
    load_place("data/placówka.csv")
    load_venue("data/sala.csv")
    load_seance("data/seans.csv")
    load_ticket("data/bilet.csv")
    load_role("data/rola.csv")
    load_discount("data/ulga.csv")
    load_uzytkownik("data/użytkownik.csv")
    load_transaction("data/transakcja.csv")
    load_product("data/produkt.csv")
    load_product_transaction("data/produkt_transakcja.csv")
