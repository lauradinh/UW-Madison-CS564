SELECT COUNT(DISTINCT(Category_Name))
FROM Category,
    Bid
WHERE Category.ItemId = Bid.ItemId
    AND Bid.Amount != "NULL"
    AND Bid.Amount > 100;