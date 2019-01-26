from google.cloud import bigquery
from config import vars
from config import schema_fike

class CreateTables:

    def __init__(self):
        self.project_id=vars.PROJECT_ID
        self.staging_dataset=vars.STAGING_DATASET

    def createtable(self):
        client = bigquery.Client(self.project_id)
        dataset_ref = client.dataset(self.staging_dataset)
        table_ref = dataset_ref.table('CRIME_RATE')
        schema=[]
        for column in schema_fike.schema:
            name=column["name"]
            type=column["type"]
            required=column["required"]
            schema.append(bigquery.SchemaField(name,type,required))

        table = bigquery.Table(table_ref, schema=schema)
        if client.get_table(table_ref=table_ref):
            print("Table already exists")
        else:
            client.create_table(table)


createtablesobj=CreateTables()
createtablesobj.createtable()