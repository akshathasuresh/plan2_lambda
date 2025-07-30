"""         DO NOT DELETE           """
"""
RedactStreamHandler class overrides the logging handler and writes the logs to standard output. Custom_RedactFilter is imported and added to the handler to filter the pattern.
"""

import re
import logging
from src.awslambda.custom_redact_filter import Custom_RedactFilter, sensitive_keys

#generates a compiled regex pattern list by replacing each key present in sensitive_keys list
def get_new_pattern():
    new_pattern = []
    for key in sensitive_keys:
        temp_re = ""
        #regex is grouped by using (). Only the last group needs to be masked.
        temp_re = re.compile(rf'((^| |,)(\\*\"?\'?{key}\\*\'?\"? *)(:|=| )( *\\*\"?\'?)(SSO |JWT |Bearer )?)([\w\-|\w.]+)',re.I|re.M)
        new_pattern.append(temp_re)
    return new_pattern

#regex to filter a valid ssn without 'SSN' key.
ssn_pattern = [re.compile(r'(?!000|666)[0-8][0-9]{2}-(?!00)[0-9]{2}-(?!0000)[0-9]{4}')]
  

"""Every handler should be created from this class so that custom_redactfilter can search for the pattern in the log message and mask it. Additional filters can be added to filter different patterns and replace with custom masks. """
class RedactStreamHandler(logging.StreamHandler):
    def __init__(self,*args,**kwargs):
        logging.StreamHandler.__init__(self,*args,**kwargs)
        #To retain the group \\g<1> is used. The last group is replaced with [REDACTED].
        self.addFilter(Custom_RedactFilter(get_new_pattern() , default_mask = "\\g<1>[REDACTED]"))
        self.addFilter(Custom_RedactFilter(ssn_pattern , default_mask = "[REDACTED]"))
        

"""This Handler needs to added to the logger created in lambda_function. The basic config of logging is'%(levelname)s:%(name)s:%(message)s'. Since a custom logger and handler is created, formatter is set to retain the basic config"""
stream_handler = RedactStreamHandler()
formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
stream_handler.setFormatter(formatter)

