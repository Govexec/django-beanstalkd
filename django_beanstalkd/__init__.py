"""
Django Beanstalk Interface
"""
import json
import logging

from django.conf import settings
from django.core.mail import send_mail
from raven import Client

from beanstalkc import Connection, SocketError, DEFAULT_PRIORITY, DEFAULT_TTR

from decorators import beanstalk_job

def connect_beanstalkd(server=None, port=11300):
    """Connect to beanstalkd server(s) from settings file"""

    if server is None:
        server = getattr(settings, 'BEANSTALK_SERVER', '127.0.0.1')

    if server.find(':') > -1:
        server, port = server.split(':', 1)

    try:
        port = int(port)
        return Connection(server, port)
    except (ValueError, SocketError), e:
        raise BeanstalkError(e)


class BeanstalkError(Exception):
    pass

class BeanstalkRetryError(Exception):
    def __init__(self, msg='', email_info=None, data=None):
        try:
            self.email_address = email_info['address']
            self.email_subject = email_info['subject']
            self.email_body = email_info['body']
            self.should_email = True
        except:
            self.email_address = None
            self.email_subject = None
            self.email_body = None
            self.should_email = False

        self.data = data
        super(BeanstalkRetryError, self).__init__(msg)


class BeanstalkClient(object):
    """beanstalk client, automatically connecting to server"""

    def call(self, func, arg='', priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
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

class backoff_beanstalk_job(object):
    def __init__(self, max_retries, delay=0, priority=1, ttr=3600, warn_after=None):
        self.max_retries = max_retries
        self.warn_after = warn_after
        self.delay = delay
        self.priority = priority
        self.ttr = ttr

        self.beanstalk_job = None

    def __call__(self, f):

        class wrapper(beanstalk_job):
            u"""A retryable beanstalk job.

            Like a normal beanstalk job, except that it will attempt to retry
            the job for a max number of retries using an exponential backoff
            algorithm when BeanstalkRetry exceptions are thrown.

            It also forces the wrapped function to take a dictionary vs a
            string. The job is still created with a string argument, but the
            argument must be json serializable.
            """

            def __init__(instance):
                super(wrapper, instance).__init__(f)

            def __call__(instance, arg):
                try:
                    data = json.loads(arg)
                    attempt = int(data.pop(u'__attempt', 0))
                except (TypeError, ValueError) as e:
                    return instance.f(arg)

                try:
                    return instance.f(data)
                except BeanstalkRetryError as e:
                    try:
                        job = settings.BEANSTALK_JOB_NAME % {
                            u'app': instance.app,
                            u'job': instance.__name__,
                        }
                    except AttributeError:
                        job = u"{}.{}".format(instance.app, instance.__name__)

                    if attempt < self.max_retries:
                        if self.warn_after is not None and attempt == (self.warn_after - 1):
                            msg = u"Approaching max retry attempts for {}.".format(job)
                            warn_data = {
                                'extra': {
                                    'Job': job,
                                    'Attempt number': attempt,
                                    'Warn after': self.warn_after,
                                    'Max retries': self.max_retries,
                                    'Job data': data,
                                }
                            }
                            raven_client = Client(dsn=settings.RAVEN_CONFIG[u'dsn'])
                            raven_client.captureMessage(msg, data=warn_data, stack=True, level=logging.WARN)

                        data[u'__attempt'] = attempt + 1

                        beanstalk_client = BeanstalkClient()
                        beanstalk_client.call(job, json.dumps(data), delay=(2 ** attempt), priority=self.priority, ttr=self.ttr)
                    else:
                        msg = u"Exceeded max retry attempts for {}.".format(job)
                        error_data = e.data if e.data is not None else {}
                        raven_client = Client(dsn=settings.RAVEN_CONFIG[u'dsn'])
                        raven_client.captureMessage(msg, data=error_data, stack=True)

                        if e.should_email:
                            send_mail(e.email_subject, e.email_body, settings.DEFAULT_FROM_EMAIL, [e.email_address], fail_silently=False)
                except Exception as e:
                    raven_client = Client(dsn=settings.RAVEN_CONFIG[u'dsn'])
                    raven_client.captureException()

        return wrapper()
