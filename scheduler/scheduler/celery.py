from __future__ import absolute_import

import os
import sys

from celery import Celery
from django.conf import settings

# set the default Django settings module for the 'celery' program.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print('******')
print('******')
print(PROJECT_ROOT)
print('******')
print('******')
sys.path.append(PROJECT_ROOT)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "scheduler.settings")


app = Celery('scheduler')


app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
