SELECT COUNT(*)
FROM (
    SELECT COUNT(*) as Total
    FROM Category
    GROUP BY ItemID
) A
WHERE A.Total = 4;

