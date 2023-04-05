import datetime
import os
from os.path import exists
import pandas as pd
import xlsxwriter
import xlwings as xw


def updateData():
    if not exists('liveSheets'):
        os.mkdir('liveSheets')
    symbols = str(open('symbols.txt').read()).split("\n")
    for inputSymbol in symbols:

        sheets = ['{}'.format(inputSymbol), '{}-I'.format(inputSymbol), '{}-II'.format(inputSymbol),
                  '{}-III'.format(inputSymbol)]

        sheets.reverse()

        dfList = {}

        candleList = []

        for sheet in sheets:

            if not exists('{}/{}_tick_data.csv'.format(str(datetime.datetime.now()).split(" ")[0], sheet)):
                continue

            dfList.setdefault(sheet, pd.read_csv(
                '{}/{}_tick_data.csv'.format(str(datetime.datetime.now()).split(" ")[0], sheet)))

            # Convert the "ServerTime" column to a datetime object
            dfList[sheet]['ServerTime'] = pd.to_datetime(dfList[sheet]['ServerTime'], unit='s', utc=True).map(
                lambda x: x.tz_convert('Asia/Kolkata'))

            # Set the "ServerTime" column as the DataFrame index
            dfList[sheet].set_index('ServerTime', inplace=True)

            # Resample the DataFrame to 5-minute intervals
            candlestick = dfList[sheet].resample('5T')

            # Aggregate the 2023-04-04
            agg_dict = {
                'AverageTradedPrice': 'sum',
                'LastTradePrice': 'last',
                'LastTradeQty': 'sum',
                'OpenInterest': 'last',
                'Value': 'last',
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'TotalQtyTraded': 'last'
            }

            candlestick = candlestick.agg(agg_dict)

            # Drop any rows that have missing 2023-04-04
            candlestick.dropna(inplace=True)
            candleList.append(candlestick)

        if not exists('liveSheets/{}.xlsx'.format(inputSymbol)):
            print('liveSheets/{}.xlsx'.format(inputSymbol))
            workbook = xlsxwriter.Workbook('liveSheets/{}.xlsx'.format(inputSymbol))
            workbook.close()

        excelSheet = xw.Book('liveSheets/{}.xlsx'.format(inputSymbol))

        for s in sheets:
            try:
                excelSheet.sheets.add(s)
            except Exception as e:
                print(e)

        for i in range(4):
            try:
                excelSheet.sheets[sheets[i]].range('A1').value = candleList[i]
            except Exception as e:
                print(e)


while True:
    updateData()
