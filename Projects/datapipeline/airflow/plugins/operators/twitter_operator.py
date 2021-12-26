import sys
sys.path.append("/home/cristian/Desktop/Data_Science/Projects/datapipeline/airflow/plugins")

import json
from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join


class TwitterOperator(BaseOperator):

    #Feature from airflow which uses jinja's template to allow such fields as parameters in DAG's 
    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        conn_id = None,
        start_time = None,
        end_time = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time
        )
        
        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ == '__main__':
    #DAG = Directed Acyclic Graph
    with DAG(dag_id='TwitterTest', start_date=datetime.now()) as dag:
        twitter_operator = TwitterOperator(
            query= 'AluraOnline', 
            #file_path="AluraOnline_{{ ds_nodash }}.json", #ds is an airflow variable which return the execution_date velue. nodash removes signs
            file_path=join(
                "/home/cristian/Desktop/Data_Science/Projects/datapipeline/datalake", # path to datalake folder
                "twitter_aluraonline", # table's name 
                "extract_date={{ ds }}",
                "AluraOnline_{{ ds_nodash }}.json"
            ),
            task_id='test_run'
        )
        task_instance = TaskInstance(task=twitter_operator, execution_date=datetime.now() - timedelta(days=2))
        #twitter_operator.execute(task_instance.get_template_context()) #
        task_instance.run()