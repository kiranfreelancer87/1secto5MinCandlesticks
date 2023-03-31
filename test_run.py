import json
import pandas as pd

data = open('test.txt', 'r').read()
jsonData = json.loads(data)
headers = jsonData['Result'][0]
df = pd.DataFrame(data=jsonData['Result'])

print(df.keys())
