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
os.environ["AWS_SESSION_TOKEN"] = config["AWS_SQS_QUEUE"]["aws_session_token"]

wps_server = config["ADES_WPS-T_SERVER"]["wps_server_url"]
default_queue_url = config["AWS_SQS_QUEUE"].get('queue_url', None)
reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("reply_timeout_sec", 20))
execute_reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("execute_reply_timeout_sec", 600))
deploy_process_timeout_sec = int(config["AWS_SQS_QUEUE"].get("deploy_process_timeout_sec", 900))

default_reply_queue_name = config["AWS_SQS_QUEUE"].get('reply_queue', 'reply_queue_')
default_reply_queue = ReplyQueueFactory(
    name=default_reply_queue_name,
    access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
    secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
    session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
    region_name=config["AWS_SQS_QUEUE"]['region_name']
).build()

publisher = PublisherFactory(
    access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
    secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
    session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
    region_name=config["AWS_SQS_QUEUE"]['region_name']
).build()

reply_queue_dict = {}
reply_queue_dict[os.path.basename(default_queue_url)] = default_reply_queue


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

def get_reply_queue(queue_url):

    global reply_queue_dict

    queue_name = os.path.basename(queue_url)
    if queue_name in reply_queue_dict.keys():
        return reply_queue_dict[queue_name]

    reply_queue_name = "reply_queue_{}".format(queue_name)


    reply_queue = ReplyQueueFactory(
        name=reply_queue_name,
        access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
        secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
        session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
        region_name=config["AWS_SQS_QUEUE"]['region_name']
    ).build()
    reply_queue_dict[queue_name] = reply_queue

    return reply_queue

def submit_message(data, queue_url=default_queue_url, timeout=reply_timeout_sec):

    print("\n\n\n")
    reply_queue = get_reply_queue(queue_url)
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
def getLandingPage(queue_url:str):
    data = {'job_type': const.GET_LANDING_PAGE}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def getProcesses(queue_url:str):
    print("\nGET LIST of ALL PROCESSES")
    data = {'job_type': const.GET_PROCESSES}
    response = submit_message(data, queue_url)
    proc_list = []
    #data = json.loads(response)
    processes = response["processes"]
    for d in processes:
        proc_list.append(d["id"])
    print("\n\nProcesses: {}".format(proc_list))
    # print(json.dumps(response, indent=2))
    return proc_list

@app.command()
def deployProcess(payload:str, queue_url:str):
    data = {'job_type': const.DEPLOY_PROCESS, 'payload_data' : payload}
    response = submit_message(data, queue_url, timeout=deploy_process_timeout_sec)
    print(json.dumps(response, indent=2))

@app.command()
def getProcessDescription(process_id: str, queue_url:str):
    print(process_id)
    data = {'job_type': const.GET_PROCESS_DESCRIPTION, 'process_id' : process_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def undeployProcess(process_id: str, queue_url:str):
    print(process_id)
    data = {'job_type': const.UNDEPLOY_PROCESS, 'process_id' : process_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def getJobList(process_id: str, queue_url:str):
    print("\nGET ALL JOBS for PROCESS : {}".format(process_id))
    data = {'job_type': const.GET_JOB_LIST, 'process_id' : process_id}
    response = submit_message(data, queue_url)
    job_list = []
    #data = json.loads(response)
    jobs = response["jobs"]
    for j in jobs:
        job_list.append(j["jobID"])
    print("\n\nJobs for process {} : {}".format(process_id, job_list))
    #print(json.dumps(response, indent=2))
    return job_list

@app.command()
def execute(process_id: str, payload_data: str, queue_url:str):
    print(process_id)
    if os.path.exists(payload_data):
        with open(payload_data, 'r') as f:
            payload = json.load(f)
    else:
        payload =  payload_data
    data = {'job_type': const.EXECUTE, 'process_id' : process_id, 'payload_data' : payload}
    response = submit_message(data, queue_url, timeout=execute_reply_timeout_sec)
    print(json.dumps(response, indent=2))

@app.command()
def getStatus(process_id: str, job_id:str, queue_url:str):
    print("\nGET STATUS of PROCESS : {} JOB : {}".format(process_id, job_id))
    data = {'job_type': const.GET_STATUS, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def dismiss(process_id: str, job_id:str, queue_url:str):
    print(process_id)
    data = {'job_type': const.DISMISS, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def getResult(process_id: str, job_id:str, queue_url:str):
    print(process_id)
    data = {'job_type': const.GET_RESULT, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))
    
@app.command()
def fullResult(queue_url:str):
    processes = getProcesses(queue_url)
    jobs = {}
    for p in processes:
        jobs = getJobList(p, queue_url)
        for j in jobs:
            getStatus(p, j, queue_url)
    

if __name__=="__main__":
    sqs_config={}
    for key in config["AWS_SQS_QUEUE"]:
        sqs_config[key] = config["AWS_SQS_QUEUE"][key]
    if config["AWS_SQS_QUEUE"][key].isnumeric():
        sqs_config[key] = int(config["AWS_SQS_QUEUE"][key])

    app()
