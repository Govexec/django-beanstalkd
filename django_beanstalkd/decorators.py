# -*- coding: utf-8 -*-
import json
import logging
import sys

from django.conf import settings
from django.core.mail import send_mail
from raven import Client as RavenClient

from .client import BeanstalkClient
from .errors import BeanstalkRetryError


class beanstalk_job(object):
    """
    Decorator marking a function inside some_app/beanstalk_jobs.py as a
    beanstalk job
    """

    def __init__(self, f):
        modname = f.__module__
        self.f = f
        self.__name__ = f.__name__
        self.__module__ = modname

        # determine app name
        parts = f.__module__.split('.')
        if len(parts) > 1:
            self.app = parts[-2]
        else:
            self.app = ''

        # store function in per-app job list (to be picked up by a worker)
        __import__(modname)
        bs_module = sys.modules[modname]
        try:
            if self not in bs_module.beanstalk_job_list:
                bs_module.beanstalk_job_list.append(self)
        except AttributeError:
            bs_module.beanstalk_job_list = [self]

    def __call__(self, arg):
        # call function with argument passed by the client only
        return self.f(arg)


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
                            raven_client = RavenClient(dsn=settings.RAVEN_CONFIG[u'dsn'])
                            raven_client.captureMessage(msg, data=warn_data, stack=True, level=logging.WARN)

                        data[u'__attempt'] = attempt + 1

                        beanstalk_client = BeanstalkClient()
                        beanstalk_client.call(job, json.dumps(data), delay=(2 ** attempt), priority=self.priority, ttr=self.ttr)
                    else:
                        msg = u"Exceeded max retry attempts for {}.".format(job)
                        error_data = e.data if e.data is not None else {}
                        raven_client = RavenClient(dsn=settings.RAVEN_CONFIG[u'dsn'])
                        raven_client.captureMessage(msg, data=error_data, stack=True)

                        if e.should_email:
                            send_mail(e.email_subject, e.email_body, settings.DEFAULT_FROM_EMAIL, [e.email_address], fail_silently=False)
                except Exception as e:
                    raven_client = RavenClient(dsn=settings.RAVEN_CONFIG[u'dsn'])
                    raven_client.captureException()

        return wrapper()
