import argparse
import os
from icecream import ic
ic.configureOutput(prefix='|> ')

import uvicorn
from DataStore import get_dsn, load_db, DB_NAME
from app import app
from price_calculator.ComputeFeed import compute_feed_price

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", action="store", default="C:/test/gvr_inputs")
parser.add_argument("--load_db", action="store_true", default=True)
parser.add_argument("--start", action="store", default="")
parser.add_argument("--end", action="store", default="")
parser.add_argument("--book_date", action="store", default="")
parser.add_argument("--external_id", action="store", default="")
parser.add_argument("--web_ui", action="store_true", default=True)

if __name__ == "__main__":
    args = parser.parse_args()
    if args.web_ui is False:
        dsn = get_dsn(DB_NAME)
        if args.load_db:
            db_load_results = load_db(os.path.abspath(args.input_path), dsn)
        start = args.start if len(args.start) > 0 else "2024-02-21"
        end = args.end if len(args.end) > 0 else "2024-02-26"
        external_id = args.external_id if len(args.external_id) > 0 else "orp5b45c10x"
        book_date = args.book_date if len(args.book_date) > 0 else "2023-10-12"
        calculated_feed_price = compute_feed_price(external_id, start, end, book_date, dsn)
        ic(calculated_feed_price)
    else:
        uvicorn.run(app, host="0.0.0.0", port=8080)
