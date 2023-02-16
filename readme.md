# Docker-airflow application for web scrapping stock and US economics data

# Description
This is a local airflow application set up with MySQL database using docker for webscraping data daily for stock(Ticker'SPY')
from [yahoo finance](https://uk.finance.yahoo.com/) and US economics data from [Investing.com](https://www.investing.com/)
Module used in dags are copied from mypackage every time the airflow image is built
the module used for this package is developed in my other package [stock_and_econ_data_ETL](https://github.com/garys2022/stock_and_econ_data_ETL)

# how to install this application
1. Clone this repository into your local machine
2. Create repo 'mypackage' under this repo
3. Clone [stock_and_econ_data_ETL](https://github.com/garys2022/stock_and_econ_data_ETL) in side 'mypackage'
4. Go back to this repo in your local machine
5. Build docker image (Commend: Docker build . -t extending_airflow:latest)
6. Docker compose (this get the set of server start running)
7. If this is the first time build up , run the init_db function under stock_and_con_data_ETL.model.db to initialize the db
8. login to the airflow application in localhost:8080
9. Set up the database config in airflow for the database that you want to store the stock and economics data with the name 'mysqldb'
(you can always use the name you want but the connection id inside the dag shall also be updated)
10. The airflow application shall be ready to use now.

last update: 2023/02/16