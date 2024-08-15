#Personal Development Environment for DBT at Toucan. 

Fork from ToucanToco/data-finance-dbt

Follow these steps to quickly set up your DBT dev environment: 

1) Create a BigQuery dataset under the project data-finance-staging with the naming convention dbt_firstname_lastname

2) Create a new service account in GCP with the BigQuery Admin rights

3) Generate a JSON key for this new service account, store this key in Bitwarden and locally on your computer. 

4) git clone this repo

5) Modify the profile.yml file by changing the fields keyfile and dataset

6) execute the following commands inside the cloned repo to create a virtual Python environment and install the necessary packages 

python3 -m pip install virtualenv
python3 -m venv ~/Documents/virtual_envs/dbt
source ~/Documents/virtual_envs/dbt/bin/activate
pip install -r requirements.txt

7) Delete all files in the model folder outside the dim_date_bis.sql

8) Check the BigQuery connexion is working with the command dbt debug

9) Execute dbt run
=> This should create a new data model in your BigQuery dataset under the project data-finance-staging  
