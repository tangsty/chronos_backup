import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
import ujson
from StringIO import StringIO
from time import strftime, localtime, time
import argparse


def backup(url, log_file, user, password, auth_simple, auth_digest):
    url += '/scheduler/jobs'
    r = get_req(url, user, password, auth_simple, auth_digest)
    if not r or r.status_code != 200:
        print("  FAILED to get json from %s: %d" % (url, r.status_code))
        return
    time_tag = strftime("%Y%m%d_%H%M%S", localtime(time()))
    output_log_location = log_file + '_' + time_tag + '.json'
    with open(output_log_location, 'w') as output:
        output.write(r.text)
    print("Backup json saved into %s" % output_log_location)


def get_req(url, user, password, auth_simple, auth_digest):
    r = None
    if user:
        if auth_simple:
            r = requests.get(url, auth=HTTPBasicAuth(user, password))
        elif auth_digest:
            r = requests.get(url, auth=HTTPDigestAuth(user, password))
    if r is None:
        print("NO authentication")
        r = requests.get(url)
    return r


def restore(url, log_file_location, user, password, auth_simple, auth_digest):
    for line in open(log_file_location):
        io = StringIO(line)
        json_decode = ujson.load(io)
        for item in json_decode:
            # check if the job is a dependency
            if 'parents' in item:
                payload = ujson.dumps(item)
                url_to_send = url + '/scheduler/dependency'
                headers = {'Content-type': 'application/json'}
                r = post_req(url_to_send, payload, headers, user, password,
                             auth_simple, auth_digest)
            else:
                payload = ujson.dumps(item)
                url_to_send = url + '/scheduler/iso8601'
                headers = {'Content-type': 'application/json'}
                r = post_req(url_to_send, payload, headers, user, password,
                             auth_simple, auth_digest)
            if not r or r.status_code != 200:
                print("  FAILED to post json to %s: %d" % (url, r.status_code))
                return
            print (r.text)


def post_req(url_to_send, payload, headers, user, password, auth_simple,
             auth_digest):
    r = None
    if user:
        if auth_simple:
            r = requests.post(url_to_send,
                              auth=HTTPBasicAuth(user, password),
                              data=payload, headers=headers)
        elif auth_digest:
            r = requests.post(url_to_send,
                              auth=HTTPDigestAuth(user, password),
                              data=payload, headers=headers)
    if r is None:
        print("NO authentication")
        r = requests.post(url_to_send,
                          data=payload, headers=headers)
    return r


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
    parser.add_argument('-s', '--simple', action="store_true", default=False,
                        help="http simple authentication")
    parser.add_argument('-d', '--digest', action="store_true", default=False,
                        help="http digest authentication")
    args = parser.parse_args()
    if args.simple and args.digest:
        print("CANNOT use simple and digest mode on same time!")
        return
    if not args.simple and not args.digest and args.user:
        print("Please prefer the auth mode!")
        return

    if args.backup is not None:
        backup(args.url, args.backup, args.user, args.password,
               args.simple, args.digest)
    elif args.restore is not None:
        restore(args.url, args.restore, args.user, args.password,
                args.simple, args.digest)
    else:
        print("NO action!")


if __name__ == "__main__":
    main()
