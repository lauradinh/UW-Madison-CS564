SELECT COUNT(DISTINCT(User.UserId))
FROM User, Item
WHERE User.UserId = Item.UserId
AND User.Rating > 1000;
