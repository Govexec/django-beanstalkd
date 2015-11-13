import json

from django.db import models


class JobData(models.Model):
    data = models.TextField()
    job_name = models.CharField(max_length=255)

    @property
    def data_dict(self):
        try:
            return json.loads(self.data)
        except:
            return {}

    @data_dict.setter
    def data_dict(self, val):
        self.data = json.dumps(val)
