import json
import sys
import yaml
import os
from jinja2 import Template
from subprocess import check_output, run, CalledProcessError
import time
import logging


def read_config(config_file):
    input_file = open(config_file, 'rb')
    return yaml.load(input_file.read())


def check_env_variables():
    need_env = ['STOLONCTL_CLUSTER_NAME', 'STOLONCTL_STORE_BACKEND', 'STOLONCTL_STORE_ENDPOINTS']
    for ne in need_env:
        if ne not in os.environ:
            sys.stderr.write("Please set {} environment variable".format(ne))
            sys.exit(1)


# Servers accepts a JSON from stolonctl utility and returns list of servers available to connect
class Servers:
    def __init__(self, stolon_json, fallback_to_master=False):
        self.fallback_to_master = fallback_to_master
        self.standby_list = list()

        # Adding support for newer version stolon clusterdata format
        if 'DBs' in stolon_json:
            key = 'DBs'
        else:
            key = 'dbs'

        # get standby's
        for db in stolon_json[key]:
            database = stolon_json[key][db]
            if 'healthy' in database['status'] and 'listenAddress' in database['status']:
                if database['status']['healthy']:
                    if database['spec']['role'] == 'standby':
                        self.standby_list.append(
                                      database['status']['listenAddress'] + ':' + database['status']['port'])
                    else:
                        self.master = database['status']['listenAddress'] + ':' + database['status']['port']

    def get_standby_list(self):
        if self.fallback_to_master and self.master is not None and self.master not in self.standby_list:
            self.standby_list.append(self.master)
        return self.standby_list


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: %s <yaml config>" % sys.argv[0])
        sys.exit(-1)

    # read config
    config = read_config(sys.argv[1])
    check_env_variables()

    while True:
        try:
            stolon_json = json.loads(check_output("stolonctl clusterdata read", shell=True))
            haproxy_template = open('./stolon_haproxy.j2', 'r')

            servers = Servers(stolon_json, config['fallback_to_master'])

            # if np servers to route - skip this iteration and print the error
            if servers.standby_list == []:
                logging.error("No available backends!")
                continue

            template = Template(haproxy_template.read())
            new_render = template.render(master=servers.master,
                                         pg_servers=servers.get_standby_list(),
                                         frontend_master_port=config['postgres_master_haproxy_port'],
                                         frontend_standby_port=config['postgres_standby_haproxy_port'],
                                         fall_count=config['inter_timeout_ms'],
                                         rise_count=config['rise_count'])

            haproxy_config = open(config['postgres_haproxy_config'], 'r')
            if haproxy_config.read() == new_render:
                logging.info("Config not changed!")
            else:
                logging.info("Config changed!")
                haproxy_config.close()
                haproxy_config = open(config['postgres_haproxy_config'], 'w')
                haproxy_config.write(new_render)
                run(config['haproxy_reload_command'], shell=True, check=True)

            haproxy_config.close()
            haproxy_template.close()

        except CalledProcessError as e:
            print(e)

        time.sleep(config['timeout'])
