CREATE SCHEMA IF NOT EXISTS `d_testdev`;

CREATE OR REPLACE VIEW `d_testdev.myview1`
AS
SELECT *
FROM `dept-sql-runner-sandbox.sqlrunner.test_data`;