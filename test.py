from vnstock import *
ticker = Listing().all_symbols()
ticker = list(ticker[:]['ticker'])
print(ticker)
