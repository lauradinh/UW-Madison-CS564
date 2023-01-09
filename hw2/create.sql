drop table if exists Item;
CREATE TABLE Item (
    ItemId INTEGER NOT NULL,
    UserId TEXT,
    Name TEXT,
    Number_of_Bids INTEGER,
    First_Bid REAL,
    Buy_Price REAL,
    Currently REAL,
    Started DATETIME,
    Ends TEXT,
    Description TEXT,
    PRIMARY KEY(ItemId)
);
drop table if exists User;
CREATE TABLE User (
    UserId TEXT NOT NULL UNIQUE,
    Location TEXT,
    Country TEXT,
    Rating INTEGER,
    PRIMARY KEY(UserId)
);
drop table if exists Category;
CREATE TABLE Category (
    Category_Name TEXT NOT NULL,
    ItemId INTEGER NOT NULL,
    FOREIGN KEY (ItemId) REFERENCES Item(ItemId) PRIMARY KEY(Category_Name, ItemId)
);
drop table if exists Bid;
CREATE TABLE Bid (
    ItemId INTEGER NOT NULL,
    UserId INTEGER NOT NULL,
    Time DATETIME,
    Amount REAL NOT NULL,
    FOREIGN KEY(ItemId) REFERENCES Item(ItemId) FOREIGN KEY(UserId) REFERENCES User(UserId) PRIMARY KEY(ItemId, UserId, Amount)
);
