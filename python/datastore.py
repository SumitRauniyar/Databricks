from google.cloud import datastore
from config import vars
import datetime

class Datastore:

    def __init__(self):
        self.project_id=vars.PROJECT_ID
        self.client=datastore.Client(self.project_id)

    def add_task(self):
        file_name='NTI_PRL_TD_TUE_LS'
        key=self.client.key('rsch_nlsn_ntl_load',file_name )
        task=datastore.Entity(key)
        print(key)
        task.update({
            'created': datetime.datetime.utcnow(),
            'description': 'first in class',
            'done': True
        })
        self.client.put(task)

    def fetch_entity(self):
        query=self.client.query(kind='rsch_nlsn_ntl_load')
        query.add_filter('Key','=',"Key('rsch_nlsn_ntl_load', 'NTI_PRL_TD_TUE_LS')")
        m=list(query.fetch())
        print(m)

datastoreobj=Datastore()
datastoreobj.add_task()
datastoreobj.fetch_entity()

print("Done")
