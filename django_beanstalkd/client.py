import beanstalkc
from django.conf import settings
from raven.contrib.django.raven_compat.models import client as raven_client

from .connection import connect_beanstalkd
from .models import JobData


class BeanstalkClient(object):
    """beanstalk client, automatically connecting to server"""

    def call(self, func, arg='', priority=beanstalkc.DEFAULT_PRIORITY, delay=0, ttr=beanstalkc.DEFAULT_TTR):
        """
        Calls the specified function (in beanstalk terms: put the specified arg
        in tube func)

        priority: an integer number that specifies the priority. Jobs with a
                  smaller priority get executed first
        delay: how many seconds to wait before the job can be reserved
        ttr: how many seconds a worker has to process the job before it gets requeued
        """
        self._beanstalk.use(func)
        self._beanstalk.put(str(arg), priority=priority, delay=delay, ttr=ttr)

    def current_jobs_delayed(self, func):
        stats = self._beanstalk.stats_tube(func)
        return stats["current-jobs-delayed"]

    def current_jobs_ready(self, func):
        stats = self._beanstalk.stats_tube(func)
        return stats["current-jobs-ready"]

    def __init__(self, **kwargs):
        server = kwargs.get('server', None)
        port = kwargs.get('port', None)
        self._beanstalk = connect_beanstalkd(server, port)


class DataBeanstalkClient(BeanstalkClient):
    def call(self, func, data_dict, *args, **kwargs):
        try:
            data = JobData()
            data.data_dict = data_dict
            data.job_name = func
            data.save()
            kwargs['arg'] = str(data.pk)
            super(DataBeanstalkClient, self).call(func, **kwargs)
        except Exception:
            raven_client.captureException()
