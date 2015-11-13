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
