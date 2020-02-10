"""
...
"""

from openeew.data.aws import AwsDataClient
from subprocess import Popen
import time
from datetime import datetime, timedelta
from configparser import ConfigParser

config = ConfigParser()
config.read_file(open('kinesis.cfg'))
OUTPUT_ACCEL_STREAM = config.get('KINESIS', 'output_accel_stream')
OUTPUT_WARNING_STREAM = config.get('KINESIS', 'output_warning_stream')

def get_active_devices(date_utc : str) -> list:
    """ get active seismic devices """
    data_client = AwsDataClient('mx')

    devices = data_client.get_devices_as_of_date(date_utc)
    print(len(devices))
    active = [device['device_id'] for device in devices]
    print(active)
    return active


def main():
    # setup dockers for consumers of 2 output streams
    stream_names = [OUTPUT_WARNING_STREAM, OUTPUT_ACCEL_STREAM]
    for stream in stream_names:
        cmd = ['docker-compose', 'run', '-e', 'STREAM_NAME={}'.format(stream), 'seismic-consumer']
        with open("/tmp/output{}.log".format(stream), "a") as output:
            process = Popen(cmd, stdout=output, stderr=output)

    date_utc = '2020-01-05 00:00:01'
    date_roll = datetime.strptime(date_utc, '%Y-%m-%d %H:%M:%S')
    interval = timedelta(days=1)

    active_set = set()

    while date_roll < datetime.now():
        # set up active seismic devices as stream producers in docker containers
        active = get_active_devices(date_utc)
        new_set = set(active)
        to_activate = new_set - active_set

        for device in to_activate:
            cmd = ['docker-compose', 'run', '-e', 'DEVICE={}'.format(device), 'seismic-service']
            with open("/tmp/output{}.log".format(device), "a") as output:
                process = Popen(cmd, stdout=output, stderr=output)

        date_roll = date_roll + interval
        date_utc = date_roll.strftime('%Y-%m-%d %H:%M:%S')
        sleep()
        #     subprocess.call("docker-compose run -e DEVICE={}  seismic-service".format(device),
        #     shell=True, stdout=output, stderr=output)


if __name__ == "__main__":
    main()
