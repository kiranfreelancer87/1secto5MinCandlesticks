import ast
import datetime
import json
import threading
import flask
from flask import Flask
from Connect import XTSConnect
import pandas as pd

bns = {}
df = pd.read_csv("clients/clients.csv")

clientList = []

orderType = "M"  # M/L

print(df.values)


def initiateClient(i):
    try:
        source = "WEBAPI"
        """Make XTSConnect object by passing your interactive API appKey, secretKey and source"""
        xt = XTSConnect(df.iloc[i]['API_KEY'], df.iloc[i]['API_SECRET'], source)
        """Using the xt object we created call the interactive login Request"""
        lR = xt.interactive_login()
        clientList.append({"Instance": xt, "clientID": df.iloc[i]['clientID'], "Qty": df.iloc[i]['Qty']})
    except Exception as e:
        # print(e)
        clientList.append("Failed")


for j in range(len(df)):
    threading.Thread(target=initiateClient, args=(j,)).start()

check = True
while check:
    if len(clientList) == len(df):
        for c in clientList:
            if "Failed" in c:
                pass
                # print("...")
        print("Done")
        check = False

with open('bns.json', 'r') as bnF:
    bns = ast.literal_eval(bnF.read())
    bnF.close()


def numToMonth(shortMonth):
    return {
        1: 'Jan',
        2: 'Feb',
        3: 'Mar',
        4: 'Apr',
        5: 'May',
        6: 'Jun',
        7: 'Jul',
        8: 'Aug',
        9: 'Sep',
        10: 'Oct',
        11: 'Nov',
        12: 'Dec'
    }[shortMonth]


def monthToNum(shortMonth):
    return \
        {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11,
         'DEC': 12}[shortMonth]


def getSymbol(str1: str):
    index = str1[0:9]
    date = str1[9:11]
    month = monthToNum(str1[11:14])
    year = str1[14:16]
    strike = str1[16:len(str1)]

    isMonthExpiry = True

    dm = str(monthToNum(str1[11:14])) + str(date)
    if isMonthExpiry:
        dm = str1[11:14]
    symbol = "{}{}{}{}".format(index, year, dm, strike)
    return symbol


def activePosition(instrument_token: int, xt):
    positionData = json.loads(json.dumps(xt.get_position_daywise()))
    posList = positionData['result']['positionList']
    print("Total Positions", len(posList))

    if posList == 0:
        print("PosList Size 0")
        return 0
    for p in posList:
        qty_ = int(str(p['Quantity']).replace(" ", ""))
        if str(instrument_token) == str(p['ExchangeInstrumentId']):
            print("Pos Qty", qty_)
        if qty_ > 0 and str(instrument_token) == str(p['ExchangeInstrumentId']):
            return int(str(p['Quantity']).replace(" ", ""))
    return 0


def getLTP(token: int, xt):
    try:
        response = xt.get_quote(
            Instruments=[
                {'exchangeSegment': 2, 'exchangeInstrumentID': token}],
            xtsMessageCode=1501,
            publishFormat='JSON')
        return float(json.loads(response['result']['listQuotes'][0])['Touchline']['LastTradedPrice'])
    except Exception as e:
        return 0.0


def getLimitPrice(token: int, side_: str, xt):
    ltp = getLTP(token, xt)
    if ltp > 0.0:
        if "B" == side_:
            trigger = float(ltp) + (float(ltp) / 100 * 3)
            return trigger
        if "S" == side_:
            trigger = float(ltp) - (float(ltp) / 100 * 3)
            return trigger
    else:
        print("Invalid Order!")
        return 0.0


def placeOrderMarket(instrument_token: int, qty_: int, side_: str, xt, clientID):
    print(instrument_token, qty_, side_, clientID)
    try:
        tradeSide = ""
        if "B" == side_:
            tradeSide = xt.TRANSACTION_TYPE_BUY
        if "S" == side_:
            tradeSide = xt.TRANSACTION_TYPE_SELL
        res = xt.place_order(
            exchangeSegment=xt.EXCHANGE_NSEFO,
            exchangeInstrumentID=instrument_token,
            productType=xt.PRODUCT_MIS,
            orderType=xt.ORDER_TYPE_MARKET,
            orderSide=tradeSide,
            timeInForce=xt.VALIDITY_DAY,
            disclosedQuantity=0,
            orderQuantity=str(qty_),
            limitPrice=0,
            stopPrice=0,
            orderUniqueIdentifier=str(datetime.datetime.now().microsecond),
            clientID=clientID)
        print(res)
        return res
    except Exception as e:
        print(e)
        return "Failed to place order!" + str(e)


def placeOrderLimit(instrument_token: int, price: float, qty_: int, side_: str, xt, clientID):
    try:
        tradeSide = ""
        if "B" == side_:
            tradeSide = xt.TRANSACTION_TYPE_BUY
        if "S" == side_:
            tradeSide = xt.TRANSACTION_TYPE_SELL
        res = xt.place_order(
            exchangeSegment=xt.EXCHANGE_NSEFO,
            exchangeInstrumentID=instrument_token,
            productType=xt.PRODUCT_MIS,
            orderType=xt.ORDER_TYPE_LIMIT,
            orderSide=tradeSide,
            timeInForce=xt.VALIDITY_DAY,
            disclosedQuantity=0,
            orderQuantity=qty_,
            limitPrice="{:.1f}".format(price),
            stopPrice=0,
            orderUniqueIdentifier=str(datetime.datetime.now().microsecond),
            clientID=clientID)
        return res
    except Exception as e:
        return "Failed to place order!" + str(e)


def placeAllOrder(strData, client):
    xt = client['Instance']
    clientID = client['clientID']
    tradeQuantity = client['Qty']
    print(client)

    try:
        print("...")
        print(strData)
        token = int(strData.split("|")[0])
        side = strData.split("|")[1]
        print(token)
        print(side)
        activePos = activePosition(token, xt)
        if "B" == side:
            print("BUY")
            if activePos > 0:
                print("Place Order")
                if orderType == "M":
                    print("Place Market Order")
                    placeOrderMarket(instrument_token=token, qty_=tradeQuantity, side_="B", xt=xt,
                                     clientID=clientID)
                if orderType == "L":
                    print("Place Limit Order")
                    placeOrderLimit(instrument_token=token, price=getLimitPrice(token, side, xt),
                                    qty_=tradeQuantity,
                                    side_="B", xt=xt, clientID=clientID)
        if "S" == side:
            print("Sell")
            if activePos > 0:
                print("Exit Order")
                if orderType == "M":
                    print("Exit With Market Order")
                    placeOrderMarket(instrument_token=token, qty_=activePos, side_="S", xt=xt,
                                     clientID=clientID)
                if orderType == "L":
                    print("Exit With Limit Order")
                    placeOrderLimit(instrument_token=token, price=getLimitPrice(token, side, xt),
                                    qty_=activePos,
                                    side_="S", xt=xt, clientID=clientID)
    except Exception as e:
        print(e)


def placeOrder(strData: str):
    for client in clientList:
        threading.Thread(target=placeAllOrder, args=(strData, client)).start()


app = Flask(__name__)


@app.route("/")
def indexPage():
    if flask.request.args.get("symbol") is None and flask.request.args.get("side") is None:
        return "Invalid Request"
    symbol = flask.request.args.get("symbol")
    side = flask.request.args.get("side")
    threading.Thread(target=placeOrder,
                     args=(str(bns.get(getSymbol(str(symbol).replace(".NFO", "")))) + "|" + side,)).start()
    return "Executing Trade", getSymbol(symbol)


app.run("127.0.0.1", port=5000)
