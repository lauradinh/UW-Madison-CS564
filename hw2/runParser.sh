rm -f *.dat
rm -f *.db
python3 parser.py ./ebay_data/items-*.json
sort -u category.dat -o category.dat
sort -u user.dat -o user.dat
sort -u bids.dat -o bids.dat
sort -u items.dat -o items.dat
sqlite3 AuctionBase.db < create.sql
sqlite3 AuctionBase.db < load.txt
