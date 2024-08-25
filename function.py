from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "cricket-stat-project"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load2",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://cricket-dataflow-metadata/udf.js",
        "JSONPath": "gs://cricket-dataflow-metadata/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "cricket-stat-project:cricket_dataset.icc_odi_batsman_ranking",
        "inputFilePattern": "gs://cricket-data-storage-fred/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://cricket-dataflow-metadata",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

