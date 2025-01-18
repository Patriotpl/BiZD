DECLARE
  numer_max NUMBER;
  nazwa_departamentu departments.department_name%TYPE := 'EDUCATION';
BEGIN
  SELECT MAX(department_id) INTO numer_max FROM departments;
  INSERT INTO departments (department_id, department_name)
  VALUES (numer_max + 10, nazwa_departamentu);
  DBMS_OUTPUT.PUT_LINE('Dodano nowy departament: ' || nazwa_departamentu || ' o numerze ' || (numer_max + 10));
END;


DECLARE
  numer_max NUMBER;
  nazwa_departamentu departments.department_name%TYPE := 'EDUCATION';
BEGIN
  SELECT MAX(department_id) INTO numer_max FROM departments;
  INSERT INTO departments (department_id, department_name, location_id)
  VALUES (numer_max + 10, nazwa_departamentu, 3000);
  DBMS_OUTPUT.PUT_LINE('Dodano nowy departament: ' || nazwa_departamentu || ' o numerze ' || (numer_max + 10));
END;


CREATE TABLE nowa (
  liczba VARCHAR2(10)
);

BEGIN
  FOR i IN 1..10 LOOP
    IF i NOT IN (4, 6) THEN
      INSERT INTO nowa VALUES (TO_CHAR(i));
    END IF;
  END LOOP;
END;


DECLARE
  kraj countries%ROWTYPE;
BEGIN
  SELECT * INTO kraj FROM countries WHERE country_id = 'CA';
  DBMS_OUTPUT.PUT_LINE('Nazwa kraju: ' || kraj.country_name);
  DBMS_OUTPUT.PUT_LINE('Region ID: ' || kraj.region_id);
END;


DECLARE
  CURSOR c_wynagrodzenia IS
    SELECT salary, last_name
    FROM employees
    WHERE department_id = 50;

  r_wynagrodzenia c_wynagrodzenia%ROWTYPE;
BEGIN
  OPEN c_wynagrodzenia;
  LOOP
    FETCH c_wynagrodzenia INTO r_wynagrodzenia;
    EXIT WHEN c_wynagrodzenia%NOTFOUND;
    IF r_wynagrodzenia.salary > 3100 THEN
      DBMS_OUTPUT.PUT_LINE(r_wynagrodzenia.last_name || ' - nie dawać podwyżki');
    ELSE
      DBMS_OUTPUT.PUT_LINE(r_wynagrodzenia.last_name || ' - dać podwyżkę');
    END IF;
  END LOOP;
  CLOSE c_wynagrodzenia;
END;


DECLARE
  CURSOR c_pracownicy (p_min_salary NUMBER, p_max_salary NUMBER, p_name_part VARCHAR2) IS
    SELECT salary, first_name, last_name
    FROM employees
    WHERE salary BETWEEN p_min_salary AND p_max_salary
      AND LOWER(first_name) LIKE '%' || LOWER(p_name_part) || '%';

  r_pracownik c_pracownicy%ROWTYPE;
BEGIN
  OPEN c_pracownicy(1000, 5000, 'a');
  LOOP
    FETCH c_pracownicy INTO r_pracownik;
    EXIT WHEN c_pracownicy%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(r_pracownik.first_name || ' ' || r_pracownik.last_name || ' - zarobki: ' || r_pracownik.salary);
  END LOOP;
  CLOSE c_pracownicy;

  OPEN c_pracownicy(5000, 20000, 'u');
  LOOP
    FETCH c_pracownicy INTO r_pracownik;
    EXIT WHEN c_pracownicy%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(r_pracownik.first_name || ' ' || r_pracownik.last_name || ' - zarobki: ' || r_pracownik.salary);
  END LOOP;
  CLOSE c_pracownicy;
END;


CREATE OR REPLACE PROCEDURE add_job(p_job_id VARCHAR2, p_job_title VARCHAR2) AS
BEGIN
  INSERT INTO jobs (job_id, job_title)
  VALUES (p_job_id, p_job_title);
  DBMS_OUTPUT.PUT_LINE('Dodano nową pracę: ' || p_job_title);
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Wystąpił błąd: ' || SQLERRM);
END;


CREATE OR REPLACE PROCEDURE update_job(p_job_id VARCHAR2, p_new_title VARCHAR2) AS
BEGIN
  UPDATE jobs
  SET job_title = p_new_title
  WHERE job_id = p_job_id;

  IF SQL%ROWCOUNT = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Nie znaleziono pracy o podanym ID.');
  END IF;

  DBMS_OUTPUT.PUT_LINE('Zaktualizowano tytuł pracy: ' || p_new_title);
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Wystąpił błąd: ' || SQLERRM);
END;


CREATE OR REPLACE PROCEDURE delete_job(p_job_id VARCHAR2) AS
BEGIN
  DELETE FROM jobs
  WHERE job_id = p_job_id;

  IF SQL%ROWCOUNT = 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 'Nie znaleziono pracy do usunięcia.');
  END IF;

  DBMS_OUTPUT.PUT_LINE('Usunięto pracę o ID: ' || p_job_id);
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Wystąpił błąd: ' || SQLERRM);
END;


CREATE OR REPLACE PROCEDURE get_employee_details(p_employee_id NUMBER, p_salary OUT NUMBER, p_last_name OUT VARCHAR2) AS
BEGIN
  SELECT salary, last_name
  INTO p_salary, p_last_name
  FROM employees
  WHERE employee_id = p_employee_id;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('Nie znaleziono pracownika o ID: ' || p_employee_id);
END;

CREATE OR REPLACE PROCEDURE add_employee(
  p_first_name VARCHAR2 DEFAULT 'John',
  p_last_name VARCHAR2 DEFAULT 'Doe',
  p_salary NUMBER DEFAULT 1000,
  p_department_id NUMBER DEFAULT 10
) AS
BEGIN
  IF p_salary > 20000 THEN
    RAISE_APPLICATION_ERROR(-20003, 'Zarobki nie mogą przekraczać 20000.');
  END IF;

  INSERT INTO employees (employee_id, first_name, last_name, salary, department_id)
  VALUES (employees_seq.NEXTVAL, p_first_name, p_last_name, p_salary, p_department_id);

  DBMS_OUTPUT.PUT_LINE('Dodano nowego pracownika: ' || p_first_name || ' ' || p_last_name);
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Wystąpił błąd: ' || SQLERRM);
END;
/