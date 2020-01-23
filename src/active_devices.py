# import subprocess
from openeew.data.aws import AwsDataClient
from openeew.data.df import get_df_from_records

from subprocess import Popen, PIPE


def main():
    data_client = AwsDataClient('mx')

    date_utc = '2020-01-05 00:00:01'
    devices = data_client.get_devices_as_of_date(date_utc)
    print(len(devices))
    active = [device['device_id'] for device in devices]
    print(active)

    # devices = ['005', '006', '007']
    for device in active[0:8]:
        cmd = ['docker-compose', 'run', '-e', 'DEVICE={}'.format(device), 'seismic-service']
        with open("/tmp/output{}.log".format(device), "a") as output:
            process = Popen(cmd, stdout=output, stderr=output)

        #     subprocess.call("docker-compose run -e DEVICE={}  seismic-service".format(device),
        #     shell=True, stdout=output, stderr=output)

if __name__ == "__main__":
    main()
