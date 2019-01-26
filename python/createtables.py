from google.cloud import bigquery

from config import vars
from config import schema
class CreateTables:

    def __init__(self):
        self.project_id=vars.PROJECT_ID
        self.staging_dataset=vars.STAGING_DATASET

    def createtable(self):
        client = bigquery.client(self.project_id)
        dataset_ref = client.dataset(self.staging_dataset)
        table_ref = dataset_ref.table('CRIME_RATE')
        schema=[]
        for column in schema.schema:
            name=column["name"]
            type=column["type"]
            required=column["required"]
            schema.append(bigquery.SchemaField(name,type,required))

        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)