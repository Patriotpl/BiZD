CREATE OR REPLACE FUNCTION STRENKOWSKIP.LIST_SEANCES_FOR_MOVIE (
    p_id_filmu NUMBER
) RETURN SYS_REFCURSOR
IS
    v_cur SYS_REFCURSOR;
BEGIN
    OPEN v_cur FOR
        SELECT f.tytuł AS film_title,
               s.id_seansu,
               s.początek  AS start_time,
               sal.id_sali AS room_id,
               p.adres_placówki AS cinema_address,
               ROW_NUMBER() OVER (PARTITION BY f.id_filmu ORDER BY s.początek) AS seance_number
          FROM film f
               JOIN seans s ON s.id_filmu = f.id_filmu
               JOIN sala sal ON s.id_sali = sal.id_sali
               JOIN placówka p ON sal.id_placówki = p.id_placówki
         WHERE f.id_filmu = p_id_filmu
         ORDER BY s.początek;
    RETURN v_cur;
END;    