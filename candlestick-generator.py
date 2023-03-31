import threading
import time
import pandas as pd
import xlwings as xw


def updateData():
    try:
        symbols = ['RELIANCE', 'HDFC', 'SBIN', 'NIFTY']
        for inputSymbol in symbols:
            sheets = ['{}'.format(inputSymbol), '{}-I'.format(inputSymbol), '{}-II'.format(inputSymbol),
                      '{}-III'.format(inputSymbol)]
            sheets.reverse()

            dfList = {}

            candleList = []

            for sheet in sheets:
                dfList.setdefault(sheet, pd.read_csv('data/{}_tick_data.csv'.format(sheet)))

                # Convert the "ServerTime" column to a datetime object
                dfList[sheet]['ServerTime'] = pd.to_datetime(dfList[sheet]['ServerTime'], unit='s', utc=True).map(
                    lambda x: x.tz_convert('Asia/Kolkata'))

                # Set the "ServerTime" column as the DataFrame index
                dfList[sheet].set_index('ServerTime', inplace=True)

                # Resample the DataFrame to 5-minute intervals
                candlestick = dfList[sheet].resample('5T')

                # Aggregate the data
                agg_dict = {
                    'Exchange': 'first',
                    'InstrumentIdentifier': 'first',
                    'AverageTradedPrice': 'sum',
                    'BuyPrice': 'sum',
                    'BuyQty': 'sum',
                    'LastTradePrice': 'last',
                    'LastTradeQty': 'last',
                    'OpenInterest': 'sum',
                    'QuotationLot': 'sum',
                    'SellPrice': 'sum',
                    'SellQty': 'sum',
                    'Value': 'last',
                    'Open': 'first',
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'TotalQtyTraded': 'sum'
                }

                candlestick = candlestick.agg(agg_dict)

                # Drop any rows that have missing data
                candlestick.dropna(inplace=True)
                candleList.append(candlestick)

            # candlestick.to_excel('LiveFeed.xlsx')

            excelSheet = xw.Book('LiveFeed.xlsx')

            for s in sheets:
                try:
                    xw.sheets.add(s)
                except Exception as e:
                    print(e)

            for i in range(4):
                excelSheet.sheets[sheets[i]].range('A1').value = candleList[i]
            time.sleep(1)
    except Exception as e:
        print(e)


while True:
    threading.Thread(target=updateData).start()
    time.sleep(1)
