# This FastPI server helps you to generate Primary Keys and Foreign Keys for postgresql tables that already existing.

## How to run the server
  ### Pull the code
    git pull <https>
  ### Update the db credentials to your wish
    cd postgres_create_pk_fk
    nano config.yaml
    update the db credentials
  ### Start the server
    python3 main



To start with,you have to analyse the existing tables,list down the constaint names,table name,column names,refernce column and tables.Refer the Sample files provided
and list down the details as per the format.

## Open Swagger on <host>:8000/docs

Before creating PK/FK, do a sanity check of data using sanity_check api
![image](https://github.com/Anand1923/postgres_create_pk_fk/assets/93506298/8083f150-0a0f-485f-bf5e-219bd1366696)

To create Primary Key reletion for multiple tables,create a csv file as per the sample format provided - Sample PK.csv.

Create primary keys and foreign keys relation to existing tables
![image](https://github.com/Anand1923/postgres_create_pk_fk/assets/93506298/fc04f564-76b4-4c6e-ad15-e586f0c2ef5d)


To create Foreign Key reletion for multiple tables,create a csv file as per the sample format provided - Sample FK.csv.

Create primary keys and foreign keys relation to existing tables
![image](https://github.com/Anand1923/postgres_create_pk_fk/assets/93506298/c50f15aa-286e-4965-a4af-55c7a0f54d98)

