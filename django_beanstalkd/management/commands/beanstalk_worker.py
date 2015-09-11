import logging
from optparse import make_option
import os
import sys
import time
import traceback

from beanstalkc import SocketError
from django import db
from django.conf import settings
from django.core.management.base import NoArgsCommand
from django_beanstalkd import BeanstalkError, connect_beanstalkd
from _mysql_exceptions import OperationalError
from raven import Client

from content_utils.utils import flush_transaction


logger = logging.getLogger('django_beanstalkd')
logger.addHandler(logging.StreamHandler())

class Command(NoArgsCommand):
    help = "Start a Beanstalk worker serving all registered Beanstalk jobs"
    __doc__ = help
    option_list = NoArgsCommand.option_list + (
        make_option('-w', '--workers', action='store', dest='worker_count',
                    default='1', help='Number of workers to spawn.'),
        make_option('-s', '--server', action='store', dest='server',
                    help='The beanstalk server to pull jobs.'),
        make_option('-p', '--port', action='store', dest='port',
                    default=11300, help='The port of the beanstalk server to pull jobs.'),
        make_option('-l', '--log-level', action='store', dest='log_level',
                    default=logging.getLevelName(logger.level), help='Log level of worker process (one of '
                    '"debug", "info", "warning", "error")'),
    )
    children = [] # list of worker processes
    jobs = {}

    def handle_noargs(self, **options):
        # set log level
        self.beanstalk_server = options['server']
        self.beanstalk_port = options['port']
        logger.setLevel(getattr(logging, options['log_level'].upper()))

        # find beanstalk job modules
        bs_modules = []
        for app in settings.INSTALLED_APPS:
            try:
                modname = "%s.beanstalk_jobs" % app
                __import__(modname)
                bs_modules.append(sys.modules[modname])
            except ImportError:
                pass
        if not bs_modules:
            logger.error("No beanstalk_jobs modules found!")
            return

        # find all jobs
        jobs = []
        for bs_module in bs_modules:
            try:
                jobs += bs_module.beanstalk_job_list
            except AttributeError:
                pass
        if not jobs:
            logger.error("No beanstalk jobs found!")
            return
        logger.info("Available jobs:")
        for job in jobs:
            # determine right name to register function with
            app = job.app
            jobname = job.__name__
            try:
                func = settings.BEANSTALK_JOB_NAME % {
                    'app': app,
                    'job': jobname,
                }
            except AttributeError:
                func = '%s.%s' % (app, jobname)
            self.jobs[func] = job
            logger.info("* %s" % func)

        # spawn all workers and register all jobs
        try:
            worker_count = int(options['worker_count'])
            assert(worker_count > 0)
        except (ValueError, AssertionError):
            worker_count = 1
        self.spawn_workers(worker_count)

        # start working
        logger.info("Starting to work... (press ^C to exit)")
        try:
            for child in self.children:
                os.waitpid(child, 0)
        except KeyboardInterrupt:
            sys.exit(0)

    def spawn_workers(self, worker_count):
        """
        Spawn as many workers as desired (at least 1).
        Accepts:
        - worker_count, positive int
        """
        # no need for forking if there's only one worker
        if worker_count == 1:
            return self.work()

        logger.info("Spawning %s worker(s)" % worker_count)
        # spawn children and make them work (hello, 19th century!)
        for i in range(worker_count):
            child = os.fork()
            if child:
                self.children.append(child)
                continue
            else:
                self.work()
                break

    def work(self):
        """children only: watch tubes for all jobs, start working"""
        try:

            while True:
                try:
                    # Reattempt Beanstalk connection if connection attempt fails or is dropped
                    beanstalk = connect_beanstalkd(server=self.beanstalk_server, port=self.beanstalk_port)
                    for job in self.jobs.keys():
                        beanstalk.watch(job)
                    beanstalk.ignore('default')

                    # Connected to Beanstalk queue, continually process jobs until an error occurs
                    # Each worker will have their own connection
                    db.connections['default'].close()
                    self.process_jobs(beanstalk)

                except (BeanstalkError, SocketError) as e:
                    msg = "Beanstalk connection error: " + str(e)
                    logger.info(msg)
                    client = Client(dsn=settings.RAVEN_CONFIG['dsn'])
                    client.captureMessage(msg, stack=True, level=logging.ERROR)

                    time.sleep(2.0)
                    logger.info("retrying Beanstalk connection...")
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    msg = "Beanstalk error: " + str(e)
                    client = Client(dsn=settings.RAVEN_CONFIG['dsn'])
                    client.captureMessage(msg, stack=True, level=logging.ERROR)

                    logger.info(msg)
                    time.sleep(2.0)
                    logger.info("retrying Beanstalk connection...")

        except KeyboardInterrupt:
            sys.exit(0)

    def process_jobs(self, beanstalk):
        while True:
            logger.debug("Beanstalk connection established, waiting for jobs")
            job = beanstalk.reserve()
            job_name = job.stats()['tube']
            if job_name in self.jobs:
                logger.debug("Calling %s with arg: %s" % (job_name, job.body))
                try:
                    connection = db.connections['default']
                    if connection.connection:
                        try:
                            connection.connection.ping()
                        except OperationalError as e:
                            connection.close()

                    flush_transaction()
                    self.jobs[job_name](job.body)
                except Exception, e:
                    tp, value, tb = sys.exc_info()
                    logger.error('Error while calling "%s" with arg "%s": '
                        '%s' % (
                            job_name,
                            job.body,
                            e,
                        )
                    )
                    logger.debug("%s:%s" % (tp.__name__, value))
                    logger.debug("\n".join(traceback.format_tb(tb)))

                    client = Client(dsn=settings.RAVEN_CONFIG['dsn'])
                    client.captureMessage(str(e), stack=True, level=logging.ERROR)

                    job.bury()
                else:
                    job.delete()
            else:
                job.release()
