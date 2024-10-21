# Stock Market ETL Producer

This is a simple FastAPI app that queries historical data for stocks from Alpha Vantage API and publishes them to a Kafka topic. The messages will be consumed [here](https://github.com/KevinSaephanh/stock-market-etl-consumer))

Tech:
- Python
- FastAPI
- Kafka