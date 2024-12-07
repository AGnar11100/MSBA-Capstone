path_to_json = './dags/connection/capstonemsba-ev-f695cb069b62.json'
project_name = "capstonemsba-ev"
gcs_service_acc = "airflow-gcp-ev@capstonemsba-ev.iam.gserviceaccount.com"
gcs_cluster_name = "process-ev-stations-plants-cluster"
my_region = 'us-west1'
my_dataset_name = "WA_EV_Dataset"
local_script_loc = "/opt/airflow/dags/data/transform.py"
local_csvs_loc = ['/opt/airflow/dags/data/ZIP-COUNTY-FIPS_2017-06.csv', '/opt/airflow/dags/data/alt_fuel_stations (Sep 21 2024).csv', '/opt/airflow/dags/data/Power_Plants.csv', '/opt/airflow/dags/data/Electric_Vehicle_Population_Size_History.csv', '/opt/airflow/dags/data/Electric_Vehicle_Population_Data_20240920.csv']
gcs_bucket_raw = 'raw-data-bucket-msba-ev-wa'
gcs_bucket_transform = 'transformed-data-bucket-msba-ev-wa'
tables_names = ['Zips_Counties', 'Charging_Stations', 'Power_Plants', 'WA_EV_Population', 'WA_EV_History']

# path_to_json = './dags/airflow-docker-age-17d71a9bbc05.json'
# project_name = "airflow-docker-age"
# gcs_service_acc = 'air-to-gcs@airflow-docker-age.iam.gserviceaccount.com'

# gcs_cluster_name = 'transform-registered-vehicles-cluster'
# my_region = 'us-west1'
# my_dataset_name = 'ca_registered_vehicles'
# local_script_loc = '/opt/airflow/dags/data/transform.py'
# local_csv_loc = '/opt/airflow/dags/data/ZIP-COUNTY-FIPS_2017-06.csv'




