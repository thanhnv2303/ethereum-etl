# Ethereum ETL

## Running in Docker

1. Install Docker https://docs.docker.com/install/
2. Install Docker compose https://docs.docker.com/compose/install/
3. Build a docker image

        > docker-compose build 
4. Create file .env look for example at file example.env

5. Run a docker compose file

        > docker-compose up

## Projects using Ethereum ETL

* [Google](https://goo.gl/oY5BCQ) - Public BigQuery Ethereum datasets
* [Nansen](https://www.nansen.ai/?ref=ethereumetl) - Analytics platform for Ethereum

## Run for dev

<<<<<<< HEAD
1. Install dependencies
   > pip3 install -r requirements.txt
   > 
2. Create file .env
=======
- [Schema](https://ethereum-etl.readthedocs.io/en/latest/schema/)
- [Command Reference](https://ethereum-etl.readthedocs.io/en/latest/commands/)
- [Documentation](https://ethereum-etl.readthedocs.io/)
- [Public Datasets in BigQuery](https://github.com/blockchain-etl/public-datasets)  
- [Exporting the Blockchain](https://ethereum-etl.readthedocs.io/en/latest/exporting-the-blockchain/)
- [Querying in Amazon Athena](https://ethereum-etl.readthedocs.io/en/latest/amazon-athena/)
- [Querying in Google BigQuery](https://ethereum-etl.readthedocs.io/en/latest/google-bigquery/)
- [Querying in Kaggle](https://www.kaggle.com/bigquery/ethereum-blockchain)
- [Airflow DAGs](https://github.com/blockchain-etl/ethereum-etl-airflow)
- [Postgres ETL](https://github.com/blockchain-etl/ethereum-etl-postgresql)
- [Ethereum 2.0 ETL](https://github.com/blockchain-etl/ethereum2-etl)
>>>>>>> b568101c9ca1c77693b7b4571940ff1523abc7ff

   > cp example.env .env
   >
3. Create mongodb

   > docker-compose up mongo-export

4. Quick run for dev

   > python3 ./quick_run/build_knowledge_graph.py

### connect mongodb for example

<<<<<<< HEAD
1. Connect hamachi network
    
   > Network id: connect_db
   > 
   > Password : bkc@123
   > 
=======
1. Install Docker: https://docs.docker.com/install/

2. Build a docker image
        
        > docker build -t ethereum-etl:latest .
        > docker image ls
        
3. Run a container out of the image
>>>>>>> b568101c9ca1c77693b7b4571940ff1523abc7ff

2. Connect mongodb (Read Only)
   

   Connection string

> main net: 
> > mongodb://readUser:bkc_123@25.39.155.190:27047/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false

> test net: 
> > mongodb://readUser:bkc_123@25.39.155.190:27037/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false
   