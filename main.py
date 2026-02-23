import streamlit as st
from datetime import date
import pymysql
conn = pymysql.connect(host="localhost", user="root", password="hema", database="Cross_Market")
import streamlit_option_menu
from streamlit_option_menu import option_menu
import pandas as pd

st.set_page_config (page_title="Cross Market Analytics", layout="wide")
st.title("📊 Cross Market Analytics")
st.caption(":blue[Crypto + Oil + Stock Market | SQL-Powered Analytics]")

queries = {
    "1)Find the top 3 cryptocurrencies by market cap":"""
      SELECT
       id,
       name,
       market_cap
FROM coins_metadata
ORDER BY market_cap DESC
LIMIT 3;
""",
"2)List all coins where ciculating supply exceeds 90% of total supply":""" 
SELECT 
    id,
    name,
    circulating_supply,
    total_supply 
FROM coins_metadata
WHERE total_supply IS NOT NULL
  AND total_supply > 0
  AND circulating_supply / total_supply > 0.9;
""",
"3)Get coins that are within 10% of their all-time-high (ATH)":"""
SELECT
    symbol,
    current_price,
    ath
FROM coins_metadata
WHERE ath IS NOT NULL
    AND current_price >= ath * 0.90;
""",
"4)Find the average market cap rank of coins with volume above $":"""
SELECT
    AVG(market_cap_rank) AS avg_market_cap_rank
FROM coins_metadata
WHERE total_volume > 1000000000;
""",
"5)Get the most recently updated coin":"""
SELECT *
FROM coins_metadata
ORDER BY last_update DESC
LIMIT 1;
""",
"6)Find the highest daily price of Bitcoin in the last 365 days":"""
SELECT MAX(price_usd) AS highest_price
FROM top3_coins_price
WHERE coin_id = 'bitcoin'
AND date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY);
""",
"7)Calculate the average daily price of Ethereum in the past 1 year":"""
SELECT 
    AVG(price_usd) AS avg_daily_price
FROM top3_coins_price
WHERE coin_id = 'ethereum'
  AND `date` >= DATE_SUB(CURDATE(), INTERVAL 365 DAY);
""",
"8)Show the daily price trend of Bitcoin in march 2025":"""
SELECT 
     coin_id,
     `date`,
     price_usd AS daily_price
FROM top3_coins_price
WHERE coin_id = 'bitcoin'
  AND `date` BETWEEN '2025-03-01' AND '2025-03-31'
ORDER BY `date`;
""",
"9)Find the coin with the highest average price over 1 year":"""
SELECT 
    coin_id,
    AVG(price_usd) AS avg_price
FROM top3_coins_price
WHERE `date` >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
GROUP BY coin_id
ORDER BY avg_price DESC
LIMIT 1;
""",
"10)Get the % change in Bitcoin’s price between feb 2025 and feb 2026": """
SELECT
    (
        MAX(CASE WHEN date = '2026-02-21' THEN price_usd END)
        -
        MAX(CASE WHEN date = '2025-02-22' THEN price_usd END)
    )
    / MAX(CASE WHEN date = '2025-02-22' THEN price_usd END) * 100
    AS percent_change
   FROM top3_coins_price
WHERE coin_id = 'bitcoin'
  AND date IN ('2025-02-22', '2026-02-21');
""",
"11)Find the highest oil price in the last 5 years":"""
SELECT MAX(price) AS highest_price
FROM oil_prices
WHERE `date` BETWEEN DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND CURDATE();
""",
"12)Get the average oil price per year":"""
SELECT 
    YEAR(`date`) AS year,
    ROUND(AVG(price), 2) AS avg_price
FROM oil_prices
GROUP BY YEAR(`date`)
ORDER BY year;
""",
"13)Show oil prices during COVID crash (March–April 2020)":"""
SELECT `date`, price
FROM oil_prices
WHERE `date` BETWEEN '2020-03-01' AND '2020-04-30'
ORDER BY `date`;
""",
"14)Find the lowest price of oil in the last 10 years":"""
SELECT 
    MIN(price) AS lowest_price
FROM oil_prices
WHERE `date` >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR);
""",
"15)Calculate the volatility of oil prices (max-min difference per year)":"""
SELECT 
    YEAR(`date`) AS year,
    ROUND(MAX(price) - MIN(price), 2) AS volatility
FROM oil_prices
GROUP BY YEAR(`date`)
ORDER BY year;
""",
"16)Get all stock prices for a given ticker":"""
SELECT Date, close
FROM stock_indices
WHERE ticker = '^GSPC'
ORDER BY Date;
""",
"17)Find the highest closing price for NASDAQ (^IXIC)":"""
SELECT MAX(`close`) 
FROM stock_indices
WHERE ticker = '^IXIC'
""",
"18)List top 5 days with highest price difference (high - low) for S&P 500 (^GSPC)":"""
ticker = "^GSPC"
SELECT `Date`, `high` - `low` AS price_diff
FROM stock_indices
WHERE ticker ="^GSPC"
ORDER BY price_diff DESC
LIMIT 5;
""",
"19)Get monthly average closing price for each ticker":"""
SELECT 
    ticker,
    DATE_FORMAT(Date, '%%Y-%%m') AS month,
    AVG(Close) AS avg_monthly_close
FROM stock_indices
WHERE ticker IN ('^GSPC','^IXIC','^NSEI')
GROUP BY ticker, DATE_FORMAT(Date, '%%Y-%%m')
ORDER BY ticker, month;
""",
"20)Get average trading volume of NSEI in 2024":"""
SELECT AVG(volume) AS avg_volume
FROM stock_indices
WHERE ticker = '^NSEI'
AND YEAR(Date) = 2024;
""",
"21)Compare Bitcoin vs Oil average price in 2025":"""
WITH btc_avg_2025 AS (
    SELECT
        AVG(price_usd) AS avg_btc_2025
    FROM top3_coins_price 
    WHERE 
        coin_id = 'bitcoin'
        AND date BETWEEN '2025-02-18' AND '2025-12-31'
),
oil_avg_2025 AS (
    SELECT
        AVG(Price) AS avg_oil_2025
    FROM oil_prices
    WHERE
        Date BETWEEN '2025-01-01' AND '2025-12-31'
)

SELECT
    btc_avg_2025.avg_btc_2025,
    oil_avg_2025.avg_oil_2025
FROM btc_avg_2025
CROSS JOIN oil_avg_2025;
""",
"22)Check if Bitcoin moves with S&P 500 (correlation idea)":"""
SELECT
    b.date,
    b.price_usd AS bitcoin_price,
    s.Close AS sp500_price
FROM top3_coins_price b
JOIN stock_indices s
    ON b.date = s.Date
WHERE b.coin_id = 'bitcoin'
  AND s.ticker = '^GSPC'
ORDER BY b.date;
""",
"23)Compare Ethereum and NASDAQ daily prices for 2025":"""
SELECT 
    c.date AS eth_date,
    c.price_usd AS eth_price,
    s.Close AS nasdaq_price
FROM top3_coins_price c
JOIN stock_indices s
    ON c.date = s.Date
WHERE c.coin_id = 'ethereum'
  AND s.ticker = '^IXIC'
  AND c.date BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY c.date;
""",
"24)Find days when oil price spiked and compare with Bitcoin price change":"""
SELECT 
    o.date AS oil_date,
    o.price AS oil_price,
    b.price_usd AS btc_price
FROM oil_prices o
JOIN top3_coins_price b
    ON o.date = b.date
WHERE b.coin_id = 'bitcoin'
ORDER BY o.date;
""",
"25)Compare top 3 coins daily price trend vs Nifty (^NSEI)":"""
SELECT 
    c.coin_id,
    c.date,
    c.price_usd,
    s.Date AS nifty_date,
    s.Close AS nifty_price
FROM top3_coins_price c
JOIN stock_indices s
    ON c.date = s.Date
WHERE c.coin_id IN ('bitcoin','ethereum','tether')
  AND s.ticker = '^NSEI'
  AND c.date BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY c.date;
""",
"26)Compare stock prices (^GSPC) with crude oil prices on the same dates":"""
SELECT 
    s.Date AS stock_date,
    s.Close AS sp500_price,
    o.date AS oil_date,
    o.price AS oil_price
FROM stock_indices s
JOIN oil_prices o
    ON s.Date = o.date
WHERE s.ticker = '^GSPC'
ORDER BY s.Date;
""",
"27)Correlate Bitcoin closing price with crude oil closing price (same date)":"""
SELECT 
    b.date,
    b.price_usd AS bitcoin_price,
    o.price AS oil_price
FROM top3_coins_price b
JOIN oil_prices o
    ON b.date = o.date
WHERE b.coin_id = 'bitcoin'
ORDER BY b.date;
""",
"28)Compare NASDAQ (^IXIC) with Ethereum price trends":"""
SELECT 
    e.date,
    e.price_usd AS ethereum_price,
    s.Close AS nasdaq_price
FROM top3_coins_price e
JOIN stock_indices s
    ON e.date = s.Date
WHERE e.coin_id = 'ethereum'
  AND s.ticker = '^IXIC'
ORDER BY e.date;
""",
"29)Join top 3 crypto coins with stock indices for 2025":"""
SELECT 
    c.coin_id,
    c.date,
    c.price_usd,
    s.ticker,
    s.Close AS stock_close
FROM top3_coins_price c
JOIN stock_indices s
    ON c.date = s.Date
WHERE c.coin_id IN ('bitcoin','ethereum','tether')
  AND s.ticker IN ('^GSPC','^IXIC','^NSEI')
  AND c.date BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY c.date, c.coin_id, s.ticker;
""",
"30)Multi-join: stock prices, oil prices, and Bitcoin prices for daily comparison":"""
SELECT 
    s.Date AS date,
    s.ticker,
    s.Close AS stock_close,
    o.price AS oil_price,
    b.price_usd AS bitcoin_price
FROM stock_indices s
JOIN oil_prices o
    ON s.Date = o.date
JOIN top3_coins_price b
    ON s.Date = b.date
WHERE s.ticker = '^GSPC'
  AND b.coin_id = 'bitcoin'
ORDER BY s.Date;
"""
}

def filters_and_exploration(conn):
    st.header("Filters & Data Exploration")

    # ---------- DATE FILTERS ----------
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Start Date",
            value=date(2025, 1, 1)
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=date(2025, 12, 31)
        )

    # ---------- AVERAGE METRICS QUERY ----------
    avg_query = f"""
        SELECT
            COALESCE(AVG(CASE WHEN t.coin_id = 'bitcoin' THEN t.price_usd END), 0) AS avg_bitcoin,
            COALESCE(AVG(o.price), 0) AS avg_oil,
            COALESCE(AVG(CASE WHEN s.ticker = '^GSPC' THEN s.close END), 0) AS avg_sp500,
            COALESCE(AVG(CASE WHEN s.ticker = '^NSEI' THEN s.close END), 0) AS avg_nifty
        FROM top3_coins_price t
        JOIN oil_prices o ON t.date = o.date
        JOIN stock_indices s ON t.date = s.date
        WHERE t.date BETWEEN '{start_date}' AND '{end_date}';
    """

    avg_df = pd.read_sql(avg_query, conn)

    # ---------- METRICS ----------
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Avg Bitcoin Price (2025)", f"${avg_df['avg_bitcoin'][0]:,.2f}")
    col2.metric("Avg Oil Price (2025)", f"${avg_df['avg_oil'][0]:,.2f}")
    col3.metric("Avg S&P 500 (2025)", f"{avg_df['avg_sp500'][0]:,.2f}")
    col4.metric("Avg Nifty 50 (2025)", f"{avg_df['avg_nifty'][0]:,.2f}")
    # ---------- DAILY SNAPSHOT TABLE ----------
    snapshot_query = f"""
        SELECT
            t.date,
            MAX(t.price_usd) AS bitcoin_price,
            MAX(o.price) AS oil_price,
            MAX(CASE WHEN s.ticker = '^GSPC' THEN s.close END) AS sp500_close,
            MAX(CASE WHEN s.ticker = '^NSEI' THEN s.close END) AS nifty_close
        FROM top3_coins_price t
        JOIN oil_prices o ON t.date = o.date
        JOIN stock_indices s ON t.date = s.date
        WHERE t.date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY t.date
        ORDER BY t.date;
    """

    snapshot_df = pd.read_sql(snapshot_query, conn)

    st.subheader("📅 Daily Market Snapshot")
    st.dataframe(snapshot_df, use_container_width=True)

def sql_query_runner(conn):
    st.header("🔹 SQL Query Runner")
    st.caption(":blue[Run predefined SQL analytics queries directly inside Streamlit]")

    # ---------- QUERY SELECTION ----------
    selected_query_name = st.selectbox(
        "Select a SQL query:",
        list(queries.keys())
    )

    st.code(queries[selected_query_name], language="sql")

    # ---------- RUN BUTTON ----------
    if st.button("▶ Run Query"):
        try:
            result_df = pd.read_sql(
                queries[selected_query_name],
                conn
            )

            st.success("Query executed successfully ✅")
            st.dataframe(result_df, use_container_width=True)

        except Exception as e:
            st.error("Error executing query ❌")
            st.write(e)

    

def crypto_analysis(conn):
    st.header("🔹 Crypto Price")
    st.caption(":blue[Analyze daily price trends for top cryptocurrencies]")

    coin = st.selectbox(
        "Select Cryptocurrency:",
        ["bitcoin", "ethereum", "tether"]
    )

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=date(2025, 1, 1),
            key="crypto_start_date"
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=date(2025, 12, 31),
            key="crypto_end_date"
        )

    query = f"""
        SELECT date, price_usd
        FROM top3_coins_price
        WHERE coin_id = '{coin}'
          AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date;
    """

    # ---------- FETCH DATA ----------
    df = pd.read_sql(query, conn)

    # ---------- VALIDATION ----------
    if df.empty:
        st.warning("No data available for the selected period.")
    else:
        st.subheader("📈 Daily Price Trend")
        st.line_chart(df.set_index("date")["price_usd"])

        st.subheader("📋 Daily Price Table")
        st.dataframe(df, use_container_width=True)


with st.sidebar:
    selected = option_menu(
        "Navigator",
        ["Filters & Data Exploration", "SQL Query Runner", "Crypto Analysis"],
        icons=["filter", "database", "graph-up"],
        default_index=0
    )
if selected == "Filters & Data Exploration":
    filters_and_exploration(conn)
elif selected == "SQL Query Runner":
       sql_query_runner(conn)    
elif selected == "Crypto Analysis":
        crypto_analysis(conn)



