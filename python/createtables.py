from google.cloud import bigquery
import time
import json
from config import vars
from config import schema_fike
from google.cloud import pubsub_v1

class CreateTables:

    def __init__(self):
        self.project_id=vars.PROJECT_ID
        self.staging_dataset=vars.STAGING_DATASET

    def createtables(self):
        client = bigquery.Client(self.project_id)
        dataset_ref = client.dataset(self.staging_dataset)
        table_ref = dataset_ref.table('CRIME_RATE_PARTITION')
        schema=[]
        for column in schema_fike.schema:
            name=column["name"]
            type=column["type"]
            required=column["required"]
            schema.append(bigquery.SchemaField(name,type,required))

        table = bigquery.Table(table_ref,schema = schema)
        table.time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='REPORTED_DATE',
            expiration_ms=7776000000,
            require_partition_filter=True
        )

        client.create_table(table)

    def pushdatatobigquery(self):
        client = bigquery.Client(self.project_id)
        dataset_ref = client.dataset(self.staging_dataset)
        table_ref = dataset_ref.table('CRIME_RATE_PARTITION')
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = 'WRITE_TRUNCATE'
        # job_config.autodetect = True
        uri = 'gs://sumitrauniyar555/crimes.csv'
        load_job=client.load_table_from_uri(uri,table_ref,job_config=job_config)
        print('Starting job {}'.format(load_job.job_id))
        load_job.result()
        print('Job finished.')
        destination_table = client.get_table(dataset_ref.table('CRIME_RATE_PARTITION'))
        print('Loaded {} rows.'.format(destination_table.num_rows))

    def pushdatatobigquerypar(self):
        client = bigquery.Client(self.project_id)
        dataset_ref = client.dataset(self.staging_dataset)
        table_ref = dataset_ref.table('CRIME_RATE_PARTITION')
        job_config = bigquery.QueryJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.destination = table_ref
        sql = """
           SELECT *
           FROM `corecompetetraining.sumitrauniyar.CRIME_RATE`
           INTO;
            """
        query_job=client.query(sql,location='US',job_config=job_config)
        query_job.result()

    def summarize(message):
        # [START parse_message]
        data = message.data.decode('utf-8')
        attributes = message.attributes

        event_type = attributes['eventType']
        bucket_id = attributes['bucketId']
        object_id = attributes['objectId']
        generation = attributes['objectGeneration']
        description = (
            '\tEvent type: {event_type}\n'
            '\tBucket ID: {bucket_id}\n'
            '\tObject ID: {object_id}\n'
            '\tGeneration: {generation}\n').format(
            event_type=event_type,
            bucket_id=bucket_id,
            object_id=object_id,
            generation=generation)

        if 'overwroteGeneration' in attributes:
            description += '\tOverwrote generation: %s\n' % (
                attributes['overwroteGeneration'])
        if 'overwrittenByGeneration' in attributes:
            description += '\tOverwritten by generation: %s\n' % (
                attributes['overwrittenByGeneration'])

        payload_format = attributes['payloadFormat']
        if payload_format == 'JSON_API_V1':
            object_metadata = json.loads(data)
            size = object_metadata['size']
            content_type = object_metadata['contentType']
            metageneration = object_metadata['metageneration']
            description += (
                '\tContent type: {content_type}\n'
                '\tSize: {object_size}\n'
                '\tMetageneration: {metageneration}\n').format(
                content_type=content_type,
                object_size=size,
                metageneration=metageneration)
        return description
        # [END parse_message]

    def poll_notifications(self,subscription_name):
        """Polls a Cloud Pub/Sub subscription for new GCS events for display."""
        # [BEGIN poll_notifications]
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(self.project_id, subscription_name)

        def callback(message):
            data = message.data.decode('utf-8')
            attributes = message.attributes
            event_type = attributes['eventType']
            bucket_id = attributes['bucketId']
            object_id = attributes['objectId']
            generation = attributes['objectGeneration']
            description = (
                '\tEvent type: {event_type}\n'
                '\tBucket ID: {bucket_id}\n'
                '\tObject ID: {object_id}\n'
                '\tGeneration: {generation}\n').format(
                event_type=event_type,
                bucket_id=bucket_id,
                object_id=object_id,
                generation=generation)
            print('Received message : {}'.format(description))
            message.ack()

        subscriber.subscribe(subscription_path, callback=callback)

        # The subscriber is non-blocking, so we must keep the main thread from
        # exiting to allow it to process messages in the background.
        print('Listening for messages on {}'.format(subscription_path))
        while True:
            time.sleep(60)

createtablesobj=CreateTables()
# createtablesobj.createtables()
# print("Table Created")
createtablesobj.pushdatatobigquerypar()
# createtablesobj.poll_notifications('bucketsubscription')
print("Data Loaded")