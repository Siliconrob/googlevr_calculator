# We are live now

## Web interface + API
- [Google Vacation Rentals Calculator Live App](https://zipzap.sfo2.digitaloceanspaces.com/gvr_calc.html)
- [Google Vacation Rentals Calculator Live API](https://google-vr-calculator.district9.info/docs)

![DigitalOcean](https://img.shields.io/badge/DigitalOcean-%230167ff.svg?style=for-the-badge&logo=digitalOcean&logoColor=white)

## Run sample inputs as command line script commands
```
zip -jr - sample_files > sample_input.zip && curl --location 'https://google-vr-calculator.district9.info/feed?external_id=rental1&start_date=2024-02-01&end_date=2024-02-08&booked_date=2023-10-21' --form 'upload_file=@"sample_input.zip"' | jq . > feed_results_$(date +%Y%m%d%H%M%S).json
```

**Individual commands with explanation**
- Build the zip file input `zip -jr - sample_files > sample_input.zip` This wipes current `sample_input.zip` file each time to avoid zip files building junk inside
- Send the input data files with parameters to compute a feed price `curl --location 'https://google-vr-calculator.district9.info/feed?external_id=rental1&start_date=2024-02-01&end_date=2024-02-08&booked_date=2023-10-21' --form 'upload_file=@"sample_input.zip"'`
- Display the JSON response in a nice human readable format `jq .`
- Send the JSON response data into a `feed_results` JSON file with timestamp uniqueness `feed_results_$(date +%Y%m%d%H%M%S).json`

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

## Interactive Web walkthrough

![unnamed](https://github.com/Siliconrob/googlevr_calculator/assets/412511/e1987069-0db0-47b2-a48b-93ccc047383e)

- Grab all the applicable ARI XML message files
- Drag and drop them on the big green box labeled drop the ARI XML files here
- If you include the Inventory File
  - ORP number loads automatically
  - Availability is parsed and loaded into the calendar
  - Changing the check in date moves calendar
- Fill in the relevant fields ORPxxxx, Arrival, Departure, and date Booking occurred
- Click the `Get Feed Price` button
- Flip between the `Formatted Results, Raw Results` tab panes

## Interactive OpenAPI walkthrough

![google_vr](https://github.com/Siliconrob/googlevr_calculator/assets/412511/563253cf-b073-415d-9e46-af4480f6522a)

- Download applicable ARI XML message files
- Zip them up `zip {name of output zip file name} {ari files pattern}` example `zip output.zip *ari*`
- Fill in the relevant fields ORPxxxx, Arrival, Departure, and date Booking occurred
