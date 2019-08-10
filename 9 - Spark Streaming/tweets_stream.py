import requests
import requests_oauthlib
import socket
import json

CONSUMER_KEY = 'INSERISCI QUI LA TUA CONSUMER KEY'
CONSUMER_SECRET = 'INSERISCI QUI LA TUA CONSUMER SECRET'
ACCESS_TOKEN = 'INSERISCI QUI IL TUO ACCESS TOKEN'
ACCESS_SECRET = 'INSERISCI QUI IL TUO ACCESS SECRET'

URL = 'https://stream.twitter.com/1.1/statuses/filter.json'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets(target):
    #query_data = [('language', 'it'), ('locations', '-130,-20,100,50'),('track',target)]
    query_url = URL+"?track="+target
    print(query_url)
    response = requests.get(query_url, auth=my_auth, stream=True)
    return response


def send_tweets(response, conn):
    for line in response.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            conn.send((tweet_text + '\n').encode())
        except Exception as e:
            print(e)


TCP_IP = "localhost"
TCP_PORT = 9999

target = input("Cosa vuoi monitorare ? ").lower()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("In attesa di una connessione TCP...")
conn, addr = s.accept()
print("Connesso ! In attesa di tweets...")
resp = get_tweets(target)
send_tweets(resp, conn)
