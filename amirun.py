import datetime
import threading
import flask
from flask import Flask
from LivePriceFeed import InitClient
import logging
import ast

try:
    with open('log.txt', 'w') as f:
        f.write('')
        f.close()
except:
    pass

logging.basicConfig(filename='log.txt', level=logging.DEBUG)
logging.debug("Script Started {}".format(datetime.datetime.now()))

'''Market Login'''
app = InitClient(API_KEY="530a7c148f5df142c05966", API_SECRET="Kxgs204$g4", source="WEBAPI",
                 CLIENT_ID="MKS5")

'''Set Your Target and Stop Loss here'''
app.setTargetAndStopLoss(Target=5, StopLoss=15)

'''Set Square Off Time'''
app.setSquareOffTime(hour=23, minute=15)

'''If Monthly Expiry make it True'''
isMonthlyExpiry = True

'''Creating instance for all clients'''
app.createClients()
bns = None

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
    if str(year) + str(month) + str(date) in str(open('bns.json', 'r').read()):
        isMonthExpiry = False
    else:
        isMonthExpiry = True
    dm = str(monthToNum(str1[11:14])) + str(date)
    if isMonthExpiry:
        dm = str1[11:14]
    symbol = "{}{}{}{}".format(index, year, dm, strike)
    return symbol


'''Create Flask App Instance'''
flask_app = Flask(__name__)

app.CurrentToken = 51722
threading.Thread(target=app.setInstruments, args=([{'exchangeSegment': 1, 'exchangeInstrumentID': 22}], 1502,)).start()
'''Default Index Page'''

'''Route to be called by AmiBroker'''


# Todo- Remove It later '''
#  Symbol Switched 0:00:00.945548, Symbol Switched 0:00:01.192190, Symbol Switched 0:00:01.028259, Symbol Switched 0:00:01.250759,

@flask_app.route("/")
def index():
    app.startTime = datetime.datetime.now()
    # TODO:
    # Ignore Sell Signal (Done)
    # Execute Order if no active order (Done)
    # If same token ignore the signal (Done)
    # If any Active Order then close old position, Open new position (Done)
    # Target - (dynamic) Ex: 10, StopLoss: (dynamic) Ex: 5 (Done)
    # Square Off Time: (dynamic) Ex: 15:15 (Done)
    # Placing Orders
    # Trade execution alert with sound

    if datetime.time(hour=int(datetime.datetime.now().hour),
                     minute=int(datetime.datetime.now().minute)) > app.SquareOffTime:
        print("Square Off Time")
        return "No More Trade for Today"

    if flask.request.args.get("symbol") is None and flask.request.args.get("side") is None:
        return "Invalid Request"
    symbol = str(flask.request.args.get("symbol")).replace(".NFO", "")
    side = flask.request.args.get("side")
    strike = symbol
    token_ = ""
    if isMonthlyExpiry:
        symbol_token_ = str(strike[:9] + strike[14:16] + str(strike[9:12][2:3] + strike[9:12][:2]) + strike[-7:])
        symbol_token_ = symbol_token_.replace(symbol_token_[11:14], str(strike[11:14]))
        token = bns[symbol_token_]
        token_ = token
    else:
        try:
            symbol_token_ = str(strike[:9] + strike[14:16] + str(strike[9:12][2:3] + strike[9:12][:2]) + strike[-7:])
            symbol_token_ = symbol_token_.replace(symbol_token_[11:14],
                                                  str(monthToNum(strike[11:14])) + str(strike[9:11]))
            token = bns[symbol_token_]
            token_ = token
        except Exception as e:
            print(e)
            pass
    print("Token" + token_ + "symbol:" + symbol)

    logging.debug("Log...Time: {}, Symbol: {}, Side: {}".format(datetime.datetime.now(), symbol, side))
    if "B" in side:
        if token_ is app.CurrentToken and app.hasActiveTrade:
            logging.debug("Log...Order Already Exists")
            return "Order Already Exists"
        if app.hasActiveTrade:
            app.ifHasActiveTrade()
        app.CurrentToken = token_
        app.AvgOrderPrice = 0
        app.hasActiveTrade = False
        if token_ is not None and len(token_) > 0:
            logging.debug("LOG...Order Placed")
            threading.Thread(target=app.setInstruments,
                             args=([{'exchangeSegment': app.xt.EXCHANGE_NSEFO,
                                     'exchangeInstrumentID': int(app.CurrentToken)}],
                                   1501,)).start()

            return "Trade Success"
    return "Option Symbol Updated Successfully"


flask_app.run('0.0.0.0', port=5000)
