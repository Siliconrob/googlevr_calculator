# We are live now
[Google Vacation Rentals Calculator Live App](https://google-vr-calculator.district9.info/docs)

![DigitalOcean](https://img.shields.io/badge/DigitalOcean-%230167ff.svg?style=for-the-badge&logo=digitalOcean&logoColor=white)

## Run sample inputs as command line script commands
```
zip -jr - sample_files > sample_input.zip && curl --location 'https://google-vr-calculator.district9.info/feed?external_id=rental1&start_date=2024-02-01&end_date=2024-02-08&booked_date=2023-10-21' --form 'upload_file=@"sample_input.zip"' | jq .
```

**Individual commands with explanation**
- Build the zip file input `zip -jr - sample_files > sample_input.zip` This wipes current `sample_input.zip` file each time to avoid zip files building junk inside
- Send the input data files with parameters to compute a feed price `curl --location 'https://google-vr-calculator.district9.info/feed?external_id=rental1&start_date=2024-02-01&end_date=2024-02-08&booked_date=2023-10-21' --form 'upload_file=@"sample_input.zip"'`
- Display the JSON response in a nice human readable format `jq .`

**Sample output**
```
{
  "total": 2605.09,
  "rent": 1867,
  "promotions": 0,
  "rate_modifiers": 0,
  "taxes_and_fees": 738.09,
  "details": {
    ...
```


## Walkthrough

![google_vr](https://github.com/Siliconrob/googlevr_calculator/assets/412511/563253cf-b073-415d-9e46-af4480f6522a)


- Download applicable ARI XML message files
- Zip them up `zip {name of output zip file name} {ari files pattern}` example `zip output.zip *ari*`
- Fill in the relevant fields ORPxxxx, Arrival, Departure, and date Booking occurred

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
- [X] FastAPI 
  - [X] Setup routes and fixup parameters
  - [X] Connect to database
  - [X] Send proper response

### Debugging to get matches
- [ ] Example messages match to specific feed prices
