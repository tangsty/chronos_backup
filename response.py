import requests
from requests.auth import HTTPBasicAuth
import ujson
from StringIO import StringIO
from time import strftime, localtime, time
import argparse


def backup(url, backup, user, password):
    url += '/scheduler/jobs'
    if user:
        r = requests.get(url, auth=HTTPBasicAuth(user, password))
    else:
        r = requests.get(url)
    time_tag = strftime("%Y%m%d_%H%M%S", localtime(time()))
    output_log_location = backup + '_' + time_tag + '.json'
    with open(output_log_location, 'w') as output:
        output.write(r.text)
    print("Backup json saved into %s" % output_log_location)


def restore(url, log_file_location, user, password):
    for line in open(log_file_location):
        io = StringIO(line)
        json_decode = ujson.load(io)
        for item in json_decode:
            # check if the job is a dependency
            if 'parents' in item:
                payload = ujson.dumps(item)
                url_to_send = url + '/scheduler/dependency'
                headers = {'Content-type': 'application/json'}
                if user:
                    r = requests.post(url_to_send,
                                      auth=HTTPBasicAuth(user, password),
                                      data=payload, headers=headers)
                else:
                    r = requests.post(url_to_send, data=payload, headers=headers)
                print (r.text)
            else:
                payload = ujson.dumps(item)
                url_to_send = url + '/scheduler/iso8601'
                headers = {'Content-type': 'application/json'}
                if user:
                    r = requests.post(url_to_send,
                                      auth=HTTPBasicAuth(user, password),
                                      data=payload, headers=headers)
                else:
                    r = requests.post(url_to_send,
                                      data=payload, headers=headers)
                print (r.text)


def main():
    parser = argparse.ArgumentParser(
        description='Backups and restores Chronos jobs. '
                    'By default, assumes you want to backup your jobs '
                    'to chronosbackups/')
    parser.add_argument('-b', '--backup',
                        help='Specifies whether to backup the Chronos jobs'
                             ' and where in the format chronosbackups/'
                             ' and the folder must already exist.')
    parser.add_argument('-r', '--restore',
                        help='Specifies whether to restore from a json'
                             ' file to the Chronos server.')
    parser.add_argument('-u', '--url',
                        help='Specify URL for Chronos in the format '
                             'http://chronos:4400',
                        required=True)
    parser.add_argument('-U', '--user', help='Specify Chronos HTTP User name',
                        default=None)
    parser.add_argument('-P', '--password',
                        help="Specify Chronos HTTP User's password",
                        default=None)
    args = parser.parse_args()
    if args.backup is not None:
        backup(args.url, args.backup, args.user, args.password)
    elif args.restore is not None:
        restore(args.url, args.restore, args.user, args.password)
    else:
        print("NO action!")


if __name__ == "__main__":
    main()
