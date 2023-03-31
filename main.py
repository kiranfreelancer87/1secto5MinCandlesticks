import datetime
import json
import os
import threading
from os.path import exists
import pandas as pd
import websocket

# now if you use logger it will not log to console.

try:
    import thread
except ImportError:
    import _thread as thread
import time

endpoint = "wss://nimblewebstream.lisuns.com:4576/"
apikey = "6d3bad57-0e07-42d1-8255-87d1bb09c06c"

tickList = {}
inputPath = 'data_2'


def Authenticate(ws):
    ws.send('{"MessageType":"Authenticate","Password":"' + apikey + '"}')


def SubscribeRealtime(ws):
    symbols = ['RELIANCE', 'HDFC', 'SBIN', 'NIFTY']

    for j in range(len(symbols)):
        Exchange = "NSE"
        InstIdentifier = symbols[j]
        Unsubscribe = False
        payload_ = {'MessageType': 'SubscribeRealtime', 'Exchange': Exchange, 'Unsubscribe': Unsubscribe,
                    'InstrumentIdentifier': InstIdentifier}
        print('Message : ' + json.dumps(payload_))
        ws.send(json.dumps(payload_))

    for j in range(len(symbols)):
        Exchange = "NFO"
        romanFormat = ["I", "II", "III"]
        for rf in romanFormat:
            InstIdentifier = symbols[j] + "-" + rf
            Unsubscribe = False
            payload_ = {'MessageType': 'SubscribeRealtime', 'Exchange': Exchange, 'Unsubscribe': Unsubscribe,
                        'InstrumentIdentifier': InstIdentifier}
            print('Message : ' + json.dumps(payload_))
            ws.send(json.dumps(payload_))


def GetHistory(ws, InstIdentifier='RELIANCE'):
    ExchangeName = "NSE"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    From = int((datetime.datetime.now() - datetime.timedelta(
        days=1)).timestamp())  # GFDL : Numerical value of UNIX Timestamp like ‘1388534400’
    To = int(datetime.datetime.now().timestamp())  # GFDL : Numerical value of UNIX Timestamp like ‘1388534400’
    isShortIdentifier = "true"

    strMessage = {'periodicity': 'TICK', 'MessageType': 'GetHistory', 'Exchange': ExchangeName,
                  'InstrumentIdentifier': InstIdentifier,
                  'From': From, 'To': To, 'UserTag': 'BN',
                  'isShortIdentifier': isShortIdentifier}

    ws.send(json.dumps(strMessage))


currentPos = 0

count = 0


def ProcessData(msg):
    global tickList
    global inputPath
    currentTickObject = []

    globalHeaders = []

    for k in dict(msg).keys():
        globalHeaders.append(k)
        currentTickObject.append(dict(msg)[str(k)])

    tickList[msg['InstrumentIdentifier']] = []
    tickList[msg['InstrumentIdentifier']].append(currentTickObject)

    save_path = "{}/{}_tick_data.csv".format(inputPath, str(msg['InstrumentIdentifier']))
    if exists(save_path):
        print("{}::::::{}".format(len(tickList[str(msg['InstrumentIdentifier'])]), str(msg['InstrumentIdentifier'])))
        df_ = pd.DataFrame(tickList[str(msg['InstrumentIdentifier'])])
        df_.drop_duplicates()
        df_.to_csv(save_path, mode='a', header=None, index=False)
    else:
        if not exists(inputPath):
            try:
                os.mkdir(inputPath)
            except Exception as e:
                print(e)
        df_ = pd.DataFrame(tickList[str(msg['InstrumentIdentifier'])])
        df_.drop_duplicates()
        df_.to_csv(save_path, header=globalHeaders, index=False)


def on_message(ws, message):
    global currentPos
    global tickList
    global count

    if len(str(message)) > 1000:
        with open('test.txt', 'w') as tf:
            tf.write(str(message))
        tf.close()

    try:
        msg = json.loads(message)
        if 'Complete' in msg and 'MessageType' and msg and msg['MessageType'] == 'AuthenticateResult':
            print("Authentication Completed")
            SubscribeRealtime(ws)
            currentPos = currentPos + 1

        if 'MessageType' in msg and msg['MessageType'] == 'RealtimeResult' and msg['PreOpen'] is False:
            threading.Thread(target=ProcessData, args=(msg,)).start()
            count = count + 1

        '''if 'Result' in msg and 'Request' in msg:
            instrument = msg['Request']['InstrumentIdentifier']
            result = msg['Result']
            print(pd.DataFrame(result).to_csv("data/{}.csv".format(instrument)))
            if currentPos == len(instruments):
                currentPos = 0
            GetHistory(ws, instruments[currentPos])
            currentPos = currentPos + 1'''

    except Exception as e:
        print("...........................")
        print(e)


def on_error(ws, error):
    print("Error")


def on_close(ws):
    print("Reconnecting...")
    websocket.setdefaulttimeout(30)
    ws.connect(endpoint)


def on_open(ws):
    # print("Connected...")
    def run(*args):
        time.sleep(1)
        print("Auth..............")
        Authenticate(ws)

    thread.start_new_thread(run, ())


websocket.enableTrace(True)

ws = websocket.WebSocketApp(endpoint,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

ws.run_forever()
