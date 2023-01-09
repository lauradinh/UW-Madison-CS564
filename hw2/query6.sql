SELECT COUNT(DISTINCT(User.UserId))
FROM User,
    Item,
    Bid
WHERE User.UserId = Item.UserId
    AND User.UserId = Bid.UserId