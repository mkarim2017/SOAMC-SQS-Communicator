import logging 
import typer
import logging
import configparser
import json
import os
import uuid

from sqs_client.factories import ReplyQueueFactory, PublisherFactory
from sqs_client.message import RequestMessage
from sqs_client.exceptions import ReplyTimeout
from constants import constants as const

logger = logging.getLogger('soamc_submitter')
logger.setLevel(logging.INFO)


CONFIG_FILER_PATH = r'sqsconfig.py'

config = configparser.ConfigParser()
config.read(CONFIG_FILER_PATH)

os.environ["AWS_ACCOUNT_ID"] = config["AWS_SQS_QUEUE"]["AWS_ACCOUNT_ID"]
os.environ["AWS_ACCESS_KEY"] = config["AWS_SQS_QUEUE"]["aws_access_key"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SQS_QUEUE"]["aws_secret_key"]
wps_server = config["ADES_WPS-T_SERVER"]["wps_server_url"]
queue_url = config["AWS_SQS_QUEUE"]['queue_url']
reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("reply_timeout_sec", 20))
execute_reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("execute_reply_timeout_sec", 600))
deploy_process_timeout_sec = int(config["AWS_SQS_QUEUE"].get("deploy_process_timeout_sec", 900))

reply_queue_name = 'reply_queue_{}'.format(os.path.basename(queue_url))
reply_queue = ReplyQueueFactory(
    name=reply_queue_name,
    access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
    secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
    region_name=config["AWS_SQS_QUEUE"]['region_name']
).build()

publisher = PublisherFactory(
    access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
    secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
    region_name=config["AWS_SQS_QUEUE"]['region_name']
).build()



'''
sh = logging.FileHandler('mylog.log')
sh.setLevel(logging.INFO)

formatstr = '[%(asctime)s - %(name)s - %(levelname)s]  %(message)s'
formatter = logging.Formatter(formatstr)

sh.setFormatter(formatter)
logger.addHandler(sh)


logging.basicConfig(level=logging.INFO)
'''

app = typer.Typer()

def submit_message(data, timeout=reply_timeout_sec):
    message = RequestMessage(
        body= json.dumps(data),
        queue_url= queue_url,
        reply_queue=reply_queue
        #group_id="SOAMC_DEFAULT_GROUP"
    )

    if queue_url.lower().endswith("fifo"):
        print("FIFO Queue: {}".format(queue_url.lower()))
        try:
            group_id=config["AWS_SQS_QUEUE"]['fifo_group_id']
        except:
            group_id = "SOAMC_DEFAULT_GROUP"

        data["uuid"] = uuid.uuid4().hex
        message = RequestMessage(
            body= json.dumps(data),
            queue_url= queue_url,
            reply_queue=reply_queue,
            group_id=group_id
    )

    print("submit_message : queue_url : {} reply_queue : {} data : {}".format(queue_url, reply_queue, json.dumps(data)))
    publisher.send_message(message)
    print("submit_message : sent")

    try:
        response = message.get_response(timeout=20)
        #print(response.body)
        return json.loads(response.body)
    except ReplyTimeout:
        return {"Error:": "Timeout"}
    except Exception as e:
        return {"Error": str(e)}
    finally:
        reply_queue.remove_queue()

@app.command()
def getLandingPage():
    data = {'job_type': const.GET_LANDING_PAGE}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def getProcesses():
    data = {'job_type': const.GET_PROCESSES}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def deployProcess(payload:str):
    data = {'job_type': const.DEPLOY_PROCESS, 'payload_data' : payload}
    response = submit_message(data, timeout=deploy_process_timeout_sec)
    print(json.dumps(response, indent=2))

@app.command()
def getProcessDescription(process_id: str):
    print(process_id)
    data = {'job_type': const.GET_PROCESS_DESCRIPTION, 'process_id' : process_id}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def undeployProcess(process_id: str):
    print(process_id)
    data = {'job_type': const.UNDEPLOY_PROCESS, 'process_id' : process_id}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def getJobList(process_id: str):
    data = {'job_type': const.GET_JOB_LIST, 'process_id' : process_id}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def execute(process_id: str, payload_data: str):
    print(process_id)
    if os.path.exists(payload_data):
        with open(payload_data, 'r') as f:
            payload = json.load(f)
    else:
        payload =  payload_data
    data = {'job_type': const.EXECUTE, 'process_id' : process_id, 'payload_data' : payload}
    response = submit_message(data, timeout=execute_reply_timeout_sec)
    print(json.dumps(response, indent=2))

@app.command()
def getStatus(process_id: str, job_id:str):
    print(process_id)
    data = {'job_type': const.GET_STATUS, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def dismiss(process_id: str, job_id:str):
    print(process_id)
    data = {'job_type': const.DISMISS, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

@app.command()
def getResult(process_id: str, job_id:str):
    print(process_id)
    data = {'job_type': const.GET_RESULT, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data)
    print(json.dumps(response, indent=2))

if __name__=="__main__":
    sqs_config={}
    for key in config["AWS_SQS_QUEUE"]:
        sqs_config[key] = config["AWS_SQS_QUEUE"][key]
    if config["AWS_SQS_QUEUE"][key].isnumeric():
        sqs_config[key] = int(config["AWS_SQS_QUEUE"][key])

    app()
