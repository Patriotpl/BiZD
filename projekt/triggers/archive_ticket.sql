--this trigger has been copied and adjusted to every single table besides archiwum

CREATE OR REPLACE TRIGGER STRENKOWSKIP.ARCHIVE_TICKET
	BEFORE INSERT OR UPDATE OR DELETE ON BILET
	FOR EACH ROW
DECLARE
	method_name VARCHAR2(10);
BEGIN
	method_name := CASE
		WHEN INSERTING THEN 'INSERT'
		WHEN UPDATING THEN 'UPDATE'
		WHEN DELETING THEN 'DELETE'
	END;
	dbms_output.put_line(method_name);
	dbms_output.put_line(USER);
	CREATE_ARCHIVE_ENTRY(USER, method_name, '', 'BILET');
END;