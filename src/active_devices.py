from openeew.data.aws import AwsDataClient
from subprocess import Popen, PIPE
import boto3
import time

# stream name : number of shards per stream
streams = {
    'InputReadings' :       1,
    'OutputAccelerations':  1,
    'OutputWarning':        1
}


def setup_streams():
    # credentials = boto3.Session().get_credentials()
    # connect to Kinesis
    kinesis = boto3.client('kinesis')
    # create streams
    for stream_name, shards in streams.items():
        stream = kinesis.create_stream(StreamName=stream_name, ShardCount=shards)
    # wait until all streams have Active status
    for stream_name in streams.keys():
        while kinesis.describe_stream_summary(StreamName=stream_name). \
                get('StreamDescriptionSummary').get('StreamStatus') != 'ACTIVE':
            time.sleep(2)
        print(f'Stream {stream_name} has been activated')


def get_active_devices(date_utc : str):
    data_client = AwsDataClient('mx')

    devices = data_client.get_devices_as_of_date(date_utc)
    print(len(devices))
    active = [device['device_id'] for device in devices]
    print(active)
    return active


def main():
    ## setup Kinesis streams
    # setup_streams()

    # setup dockers for consumers of 2 output streams
    stream_names = ['OutputWarning', 'OutputAccelerations']
    for stream in stream_names:
        cmd = ['docker-compose', 'run', '-e', 'STREAM_NAME={}'.format(stream), 'seismic-consumer']
        with open("/tmp/output{}.log".format(stream), "a") as output:
            process = Popen(cmd, stdout=output, stderr=output)

    ## get active seismic devices
    date_utc = '2020-01-05 00:00:01'
    active = get_active_devices(date_utc)

    for device in active:
        cmd = ['docker-compose', 'run', '-e', 'DEVICE={}'.format(device), 'seismic-service']
        with open("/tmp/output{}.log".format(device), "a") as output:
            process = Popen(cmd, stdout=output, stderr=output)

        #     subprocess.call("docker-compose run -e DEVICE={}  seismic-service".format(device),
        #     shell=True, stdout=output, stderr=output)


if __name__ == "__main__":
    main()
