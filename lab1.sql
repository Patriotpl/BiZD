CREATE TABLE DEPARTMENTS(department_id NUMBER PRIMARY KEY,
                        department_name VARCHAR(30),
                        manager_id NUMBER,
                        location_id NUMBER);

CREATE TABLE LOCATIONS(location_id NUMBER PRIMARY KEY,
                        street_adress VARCHAR(30),
                        postal_code VARCHAR(6),
                        city VARCHAR(40),
                        start_province VARCHAR(30),
                        country_id NUMBER);

CREATE TABLE COUNTRIES(country_id NUMBER PRIMARY KEY,
                        country_name VARCHAR(20),
                        region_id NUMBER);

CREATE TABLE REGION(region_id NUMBER PRIMARY KEY,
                    region_name VARCHAR(30));

CREATE TABLE EMPLOYEES (EMPLOYEE_ID NUMBER NOT NULL,
                        FIRST_NAME VARCHAR2(20), 
                        LAST_NAME VARCHAR2(20), 
                        EMAIL VARCHAR2(20), 
                        PHONE_NUMBER VARCHAR2(12),
                        HIRE_DATE DATE, 
                        JOB_ID NUMBER, 
                        SALARY NUMBER, 
                        COMMISSION_PCT NUMBER, 
                        MANAGER_ID NUMBER, 
                        DEPARTMENT_ID NUMBER, 
                        CONSTRAINT EMPLOYEES_PK PRIMARY KEY 
                        (
                            EMPLOYEE_ID 
                        )
                        ENABLE 
);

CREATE TABLE JOB_HISTORY(EMPLOYEE_ID NUMBER NOT NULL, 
                        START_DATE NUMBER NOT NULL, 
                        END_DATE DATE, 
                        JOB_ID NUMBER, 
                        DEPARTMENT_ID NUMBER, 
                        CONSTRAINT JOB_HISTORY_PK PRIMARY KEY 
                        (
                            EMPLOYEE_ID 
                        , START_DATE 
                        )
                        ENABLE);

CREATE TABLE JOBS(job_id NUMBER PRIMARY KEY,
                    job_title VARCHAR(20),
                    min_salary NUMBER,
                    max_salary NUMBER);

ALTER TABLE EMPLOYEES
ADD CONSTRAINT fk_manager
FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID);

ALTER TABLE EMPLOYEES
ADD CONSTRAINT e_d1
FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS (DEPARTMENT_ID);

ALTER TABLE EMPLOYEES
ADD CONSTRAINT e_j1
FOREIGN KEY (JOB_ID) REFERENCES JOBS (JOB_ID);

ALTER TABLE JOB_HISTORY
ADD CONSTRAINT jh_j1
FOREIGN KEY (JOB_ID) REFERENCES JOBS (JOB_ID);

ALTER TABLE JOB_HISTORY
ADD CONSTRAINT jh_e1
FOREIGN KEY (EMPLOYEE_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID);

ALTER TABLE JOB_HISTORY
ADD CONSTRAINT jh_d1
FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS (DEPARTMENT_ID);

ALTER TABLE DEPARTMENTS
ADD CONSTRAINT d_l1
FOREIGN KEY (LOCATION_ID) REFERENCES LOCATIONS (LOCATION_ID);

ALTER TABLE LOCATIONS
ADD CONSTRAINT l_c1
FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRIES(COUNTRY_ID);

ALTER TABLE COUNTRIES
ADD CONSTRAINT c_r1
FOREIGN KEY (REGION_ID) REFERENCES REGIONS(REGION_ID);