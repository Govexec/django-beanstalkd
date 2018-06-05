# -*- coding: utf-8 -*-
import json
import logging
import sys

from django.conf import settings
from django.core.mail import send_mail
from django.db import transaction
from raven.contrib.django.raven_compat.models import client as raven_client

from .client import BeanstalkClient
from .errors import BeanstalkRetryError
from .models import JobData


@transaction.commit_manually
def flush_transaction():
    transaction.commit()


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
                            raven_client.captureMessage(msg, data=warn_data, stack=True, level=logging.WARN)

                        data[u'__attempt'] = attempt + 1

                        beanstalk_client = BeanstalkClient()
                        beanstalk_client.call(job, json.dumps(data), delay=(2 ** attempt), priority=self.priority, ttr=self.ttr)
                    else:
                        msg = u"Exceeded max retry attempts for {}.".format(job)
                        error_data = e.data if e.data is not None else {}
                        raven_client.captureMessage(msg, data=error_data, stack=True)

                        if e.should_email:
                            send_mail(e.email_subject, e.email_body, settings.DEFAULT_FROM_EMAIL, [e.email_address], fail_silently=False)
                except Exception as e:
                    raven_client.captureException()

        return wrapper()


class data_beanstalk_job(object):
    def __init__(self, cleanup=True):
        self.cleanup = cleanup

    def __call__(self, f):

        class wrapper(beanstalk_job):
            u"""A beanstalk job where the data for job is stored in db."""

            def __init__(instance):
                super(wrapper, instance).__init__(f)

            def __call__(instance, pk_str):
                try:
                    flush_transaction()
                    pk = int(pk_str)
                    data = JobData.objects.get(pk=pk)
                except (TypeError, ValueError):
                    error_msg = "Invalid value for pk"
                    error_extra = {
                        "pk tried": pk_str
                    }
                    raven_client.captureMessage(error_msg, extra=error_extra, stack=True)
                except JobData.DoesNotExist:
                    error_msg = "Unable to find beanstalk job data."
                    error_extra = {
                        "pk tried": pk
                    }
                    raven_client.captureMessage(error_msg, extra=error_extra, stack=True)
                else:
                    try:
                        val = instance.f(data)
                        if self.cleanup:
                            data.delete()
                        return val
                    except Exception:
                        raven_client.captureException()

        return wrapper()


class retry_data_beanstalk_job(object):
    def __init__(self, max_retries, ttr=3600, cleanup=True):
        self.max_retries = max_retries
        self.ttr = ttr
        self.cleanup = cleanup

    def get_decorator(self):
        class retry_data_beanstalk_job_decorator(beanstalk_job):

            def __init__(instance, f):
                super(retry_data_beanstalk_job_decorator, instance).__init__(f)
                instance.attempt = None
                instance.beanstalk_data = None
                instance.jobdata_pk = None

            def __call__(instance, beanstalk_data_str):

                if not instance.load_beanstalk_data(beanstalk_data_str):
                    return

                flush_transaction()

                try:
                    data = JobData.objects.get(pk=instance.jobdata_pk)
                except JobData.DoesNotExist:
                    instance.handle_missing_data()
                else:
                    instance.run_job(data)

            @staticmethod
            def flush_transaction():
                flush_transaction()

            def get_job_name(instance):
                try:
                    job = settings.BEANSTALK_JOB_NAME % {
                        u'app': instance.app,
                        u'job': instance.__name__,
                    }
                except AttributeError:
                    job = u"{}.{}".format(instance.app, instance.__name__)
                return job

            def load_beanstalk_data(instance, beanstalk_data_str):
                try:
                    beanstalk_data = json.loads(beanstalk_data_str)

                    instance.beanstalk_data = beanstalk_data
                    instance.jobdata_pk = beanstalk_data["jobdata_pk"]
                    instance.attempt = beanstalk_data.get("attempt", 1)
                    return True
                except (TypeError, ValueError):
                    error_msg = "Unable to load json data."
                    culprit = instance.get_sentry_culprit("load_beanstalk_data")
                    error_data = {
                        "culprit": culprit,
                        "extra": {
                            "data string": beanstalk_data_str,
                        },
                    }
                    raven_client.captureMessage(error_msg, data=error_data, stack=True)
                except KeyError as e:
                    error_msg = "Missing required parameters for retry data beanstalk job."
                    culprit = instance.get_sentry_culprit("load_beanstalk_data")
                    error_data = {
                        "culprit": culprit,
                        "extra": {
                            "error": str(e),
                        }
                    }
                    raven_client.captureMessage(error_msg, data=error_data, stack=True)

                return False

            def get_sentry_culprit(instance, *args):
                culprit = '.'.join([instance.__class__.__name__] + list(args))
                return culprit

            def handle_missing_data(instance):
                job = instance.get_job_name()
                if instance.attempt < self.max_retries:
                    instance.beanstalk_data['attempt'] = instance.attempt + 1

                    backoff = 2 ** instance.attempt
                    beanstalk_client = BeanstalkClient()
                    beanstalk_client.call(job, json.dumps(instance.beanstalk_data), delay=backoff, ttr=self.ttr)
                else:
                    msg = u"Exceeded max retry attempts for {}.".format(job)
                    culprit = instance.get_sentry_culprit("handle_missing_data")
                    error_data = {
                        "culprit": culprit,
                        "extra": {
                            "Job name": job,
                            "Attempt number": instance.attempt,
                            "Beanstalk Data": instance.beanstalk_data,
                        }
                    }
                    raven_client.captureMessage(msg, data=error_data, stack=True)

            def run_job(instance, job_data_instance):
                try:
                    val = instance.f(job_data_instance.data_dict)
                    if self.cleanup:
                        job_data_instance.delete()
                    return val
                except Exception:
                    raven_client.captureException()
        return retry_data_beanstalk_job_decorator

    def __call__(self, f):
        return self.get_decorator()(f)
