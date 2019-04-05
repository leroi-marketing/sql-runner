DROP SCHEMA if exists sqlrunner CASCADE;

CREATE SCHEMA sqlrunner;

CREATE TABLE sqlrunner.x_test_data 
(
  col1   INT,
  col2   INT,
  col3   INT
);

INSERT INTO sqlrunner.x_test_data
VALUES
(
  1,
  2,
  3
);
