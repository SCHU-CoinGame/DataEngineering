import requests
import psycopg2

url = "https://api.upbit.com/v1/market/all?isDetails=true"
headers = {"accept": "application/json"}
res = requests.get(url, headers=headers)
data = res.json()

conn = psycopg2.connect(
    dbname="upbit_db",
    user="ilhan",
    password="000411",
    host="localhost"
)
cur = conn.cursor()

for market in data:
    market_code = market['market']
    korean_name = market['korean_name']
    english_name = market['english_name']
    market_warning = market.get('market_event', {}).get('warning', False)
    caution = market.get('market_event', {}).get('caution', {})

    price_fluctuations = caution.get('PRICE_FLUCTUATIONS', False)
    trading_volume_soaring = caution.get('TRADING_VOLUME_SOARING', False)
    deposit_amount_soaring = caution.get('DEPOSIT_AMOUNT_SOARING', False)
    global_price_differences = caution.get('GLOBAL_PRICE_DIFFERENCES', False)
    concentration_of_small_accounts = caution.get('CONCENTRATION_OF_SMALL_ACCOUNTS', False)

    cur.execute("""
            INSERT INTO market_codes (market, korean_name, english_name, market_warning, 
                                      price_fluctuations, trading_volume_soaring, deposit_amount_soaring, 
                                      global_price_differences, concentration_of_small_accounts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (market) DO UPDATE SET
            korean_name = EXCLUDED.korean_name,
            english_name = EXCLUDED.english_name,
            market_warning = EXCLUDED.market_warning,
            price_fluctuations = EXCLUDED.price_fluctuations,
            trading_volume_soaring = EXCLUDED.trading_volume_soaring,
            deposit_amount_soaring = EXCLUDED.deposit_amount_soaring,
            global_price_differences = EXCLUDED.global_price_differences,
            concentration_of_small_accounts = EXCLUDED.concentration_of_small_accounts
        """, (market_code, korean_name, english_name, market_warning,
              price_fluctuations, trading_volume_soaring, deposit_amount_soaring,
              global_price_differences, concentration_of_small_accounts))

conn.commit()
cur.close()
conn.close()