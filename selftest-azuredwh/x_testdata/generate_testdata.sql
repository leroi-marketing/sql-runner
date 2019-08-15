DROP SCHEMA sqlrunner CASCADE;
DROP SCHEMA d_testdev CASCADE;

CREATE SCHEMA sqlrunner;
CREATE SCHEMA d_testdev;

CREATE TABLE sqlrunner.test_data 
(
  col1   INTEGER,
  col2   INTEGER,
  col3   INTEGER
);

INSERT INTO sqlrunner.test_data
VALUES
(
  1,
  2,
  3
);
