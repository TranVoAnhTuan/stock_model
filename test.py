from vnstock import *
ticker = Listing().all_symbols()
# ticker = list(ticker["ticker"] == "VCB")
print(ticker[ticker["ticker"] == "VCB"])
