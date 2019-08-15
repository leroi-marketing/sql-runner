/*DISTRIBUTION = HASH(col1)
*/
CREATE VIEW d_testdev.myview3 
AS
SELECT col1, col2, max(123) as col4
FROM sqlrunner.test_data
GROUP BY col1, col2;