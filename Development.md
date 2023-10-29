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
- [X] Read Hotel Inventory from XML file
- [X] Load Hotel Inventory into database

### Command line options to calculate rates
- [X] Parse and add files data into sqlite
- [X] Add command line parser
- [X] Compute a feed price for specified dates
  - [X] Get applicable rates from db
  - [X] Get applicable taxes from db
  - [X] Get applicable fees from db
  - [X] Get applicable promotions from db
  - [X] Get applicable extra charges from db
  - [X] Calculate base rate
  - [X] Apply fees
  - [X] Apply taxes
  - [X] Apply promotions

### API Development
- [X] FastAPI 
  - [X] Setup routes and fixup parameters
  - [X] Connect to database
  - [X] Send proper response
  - [X] Allow feed files to send as zip file
  - [X] Allow raw files to send as group of text files

### Webpage Development
- [X] Web interface
  - [X] Input fields
  - [X] Drag and drop file set of ARI XML files
  - [X] Compute feed price
  - [X] Show raw response
  - [X] Show formatted data
  - [X] Add calendar view
  - [X] Parse XML Inventory file locally to load calendar availability
  - [X] Show availability clearly marked on calendar and rates returned
  - [X] Load ORPxxx number automatically
  - [X] Change calendar view based on check in date

### Debugging to get matches
- [X] Example messages match to specific feed prices
