# Build Google Vacation Rentals Calculator

### Read ARI XML messages into sqlite database
- [X] Read Rates from XML file
- [X] Load Rates into database
- [X] Read Rate Modifications from XML file
- [X] Load Rate Modifications into database
- [X] Read Availability from XML file
- [X] Load Availability into database
- [X] Read Taxes and Fees from XML file
- [X] Load Taxes and Fees into database
- [X] Read Promotions from XML file
- [X] Load Promotions into database
- [X] Read Extra Guests from XML file
- [X] Load Extra Guests into database

**Further work**
- [ ] Read Hotel Inventory from XML file
- [ ] Load Hotel Inventory into database

### Command line options to calculate rates
- [X] Parse and add files data into sqlite
- [X] Add command line parser
- [ ] Compute a feed price for specified dates
  - [X] Get applicable rates from db
  - [X] Get applicable taxes from db
  - [X] Get applicable fees from db
  - [ ] Get applicable promotions from db
  - [ ] Get applicable extra charges from db
  - [ ] Calculate base rate
  - [ ] Apply fees
  - [ ] Apply taxes
  - [ ] Apply promotions
