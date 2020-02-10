"""
this script starts docker containers for
    1. producers: 1 producer per active seismic device
    2. output stream consumers: output accelerations and warning accelelrations
"""

from openeew.data.aws import AwsDataClient
from subprocess import Popen
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

    # get active seismic devices
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
