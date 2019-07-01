import requests
import json


class BrokerException(Exception):
    pass


class BrokerClient:

    def __init__(self, broker_ip, broker_port, enable_auth=False, user="", password=""):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.enable_auth = enable_auth
        self.user = user
        self.password = password

    def get_status(self, job_id):
        # TODO validate job_id
        r = requests.get('http://%s:%s/submissions/%s' % (self.broker_ip, self.broker_port, job_id))
        return str(r.json()['status'])

    # FIXME: api should be fixed
    def get_execution_time(self, job_id):
        # TODO validate job_id
        r = requests.get('http://%s:%s/submissions/%s' % (self.broker_ip, self.broker_port, job_id))
        return r.json()[job_id]['execution_time']

    def stop_application(self, job_id):
        # TODO validate job_id
        body = {
            "username": self.user,
            "password": self.password,
            "enable_auth": self.enable_auth
        }

        headers = {'Content-Type': 'application/json'}
        requests.put('http://%s:%s/submissions/%s/stop' % (self.broker_ip, self.broker_port, job_id),
                     headers=headers, data=json.dumps(body))

    def terminate_application(self, job_id):
        # TODO validate job_id
        body = {
            "username": self.user,
            "password": self.password,
            "enable_auth": self.enable_auth
        }

        headers = {'Content-Type': 'application/json'}
        requests.put('http://%s:%s/submissions/%s/terminate' % (self.broker_ip, self.broker_port, job_id),
                     headers=headers, data=json.dumps(body))

    def submit(self, application_conf, monitor_conf, controller_conf, visualizer_conf):
        command = application_conf.get_command()
        image_name = application_conf.get_image_name()
        redis_workload = application_conf.get_redis_workload()
        init_size = application_conf.get_init_size()

        control_parameters = controller_conf.get_conf_dict()
        monitor_parameters = monitor_conf.get_conf_dict()
        visualizer_parameters = visualizer_conf.get_conf_dict()

        control_plugin = controller_conf.get_plugin()
        monitor_plugin = monitor_conf.get_plugin()
        visualizer_plugin = visualizer_conf.get_plugin()

        body = {
            "plugin": "kubejobs",
            "username": self.user,
            "password": self.password,
            "plugin_info": {
                "cmd": command,
                "img": image_name,
                "init_size": init_size,
                "redis_workload": redis_workload,
                "config_id": "id",
                "control_plugin": control_plugin,
                "control_parameters": control_parameters,
                "monitor_plugin": monitor_plugin,
                "monitor_info": monitor_parameters,
                # TODO: this should be read from the config file
                "enable_visualizer": visualizer_conf.enable(),
                "visualizer_plugin": visualizer_plugin,
                "visualizer_info": visualizer_parameters,
                "env_vars": {}
            },
            "enable_auth": self.enable_auth
        }

        url = "http://%s:%s/submissions" % (self.broker_ip, self.broker_port)

        headers = {'Content-Type': 'application/json'}
        r = requests.post(url, headers=headers, data=json.dumps(body))
        return r.json()["job_id"]
