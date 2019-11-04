import googleapiclient.discovery
from oauth2client.client import GoogleCredentials
from datetime import datetime
FILE_NAME_1 = ''
JOB_TAG_1 = ''
FILE_NAME_2 = ''
JOB_TAG_2 = ''
DEFAULT_LOCATION = "us-central1"
DEFAULT_ZONE = "us-central1-f"
BUCKET_NAME = ""
PROJECT_ID = ""

def run_dataflow_job(file_name):
    try:
        credentials = GoogleCredentials.get_application_default()
        dataflow = googleapiclient.discovery.build('dataflow', 'v1b3', credentials=credentials)
        now = datetime.now()
        jobTag = ""
        templatePath = ""
        if(file_name.__contains__(FILE_NAME_1)):
            templatePath = "gs://" + BUCKET_NAME + "/dataflow_template_1"
            jobTag = JOB_TAG_1
        elif(file_name.__contains__(FILE_NAME_2)):
            templatePath = ""gs://" + BUCKET_NAME + "/dataflow_template_2"
            jobTag = JOB_TAG_2
        else:
            print('Dataflow not called for file '+file_name)
            return       
        dt_string = now.strftime("%d/%m/%Y-%H:%M:%S")
        result = dataflow.projects().locations().templates().launch(
            projectId=PROJECT_ID,
            location=DEFAULT_LOCATION,
            gcsPath=templatePath,
            body={
                "jobName": jobTag+"-"+dt_string,
                "environment": { "zone": DEFAULT_ZONE },
                "parameters": {
                    "file_name": file_name,
                }
            }
        ).execute()
        print(result)
    except Exception as e:
        print(e)
        print(type(e))
            
def gcs_to_bq_runner(data, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(data['bucket']))
    print('File: {}'.format(data['name']))
    print('Metageneration: {}'.format(data['metageneration']))
    print('Created: {}'.format(data['timeCreated']))
    print('Updated: {}'.format(data['updated']))
    
    run_dataflow_job("gs://"+data['bucket']+"/"+data['name'])
