SELECT ItemId
FROM Item
WHERE Currently = (
    SELECT MAX(Currently)
    FROM Item
);
