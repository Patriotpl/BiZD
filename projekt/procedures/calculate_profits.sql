CREATE OR REPLACE PROCEDURE STRENKOWSKIP.CALCULATE_PROFITS
IS
    v_month NUMBER := 0;
    v_week  NUMBER := 0;
    v_year  NUMBER := 0;
BEGIN
	-- month
    SELECT NVL(SUM(t.bilans * (1 - NVL(u.zniżka, 0))), 0)
      INTO v_month
      FROM transakcja t
           JOIN użytkownik buyer ON buyer.id_użytkownika = t.id_nabywcy
           LEFT JOIN ulga u    ON u.id_ulgi = buyer.id_ulgi
     WHERE EXTRACT(YEAR  FROM t.data)  = EXTRACT(YEAR  FROM SYSDATE)
       AND EXTRACT(MONTH FROM t.data)  = EXTRACT(MONTH FROM SYSDATE);

    --week
    SELECT NVL(SUM(t.bilans * (1 - NVL(u.zniżka, 0))), 0)
      INTO v_week
      FROM transakcja t
           JOIN użytkownik buyer ON buyer.id_użytkownika = t.id_nabywcy
           LEFT JOIN ulga u    ON u.id_ulgi = buyer.id_ulgi
     WHERE TRUNC(t.data, 'IW') = TRUNC(SYSDATE, 'IW');	--trunc gets the week

    --year
    SELECT NVL(SUM(t.bilans * (1 - NVL(u.zniżka, 0))), 0)
      INTO v_year
      FROM transakcja t
           JOIN użytkownik buyer ON buyer.id_użytkownika = t.id_nabywcy
           LEFT JOIN ulga u    ON u.id_ulgi = buyer.id_ulgi
     WHERE EXTRACT(YEAR FROM t.data) = EXTRACT(YEAR FROM SYSDATE);

    DBMS_OUTPUT.PUT_LINE('Zysk - MIESIĄC: ' || v_month);
    DBMS_OUTPUT.PUT_LINE('Zysk - TYDZIEŃ: ' || v_week);
    DBMS_OUTPUT.PUT_LINE('Zysk - ROK:     ' || v_year);
END;