DROP VIEW IF EXISTS d_testdev.myview1;

CREATE VIEW d_testdev.myview1 
AS
SELECT *
FROM sqlrunner.test_data;