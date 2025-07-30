"""      DO NOT DELETE      """

"""
RedactingFilter class is inherited to override redact function and change the functionality if content is dict. Update the value of key if key present in sensitive keys. If content is str , re.sub is called to replace the pattern with default mask. Refer https://pypi.org/project/logredactor/ for detailed info.
"""

import re
import os
import logging 
#logredactor is the base module for RedactingFilter Class 
from logredactor import RedactingFilter

#default keys for which value needs to be masked
sensitive_keys = ['PASSWORD','PASSCODE','SSN','AUTHORIZATION','PRINCIPALID','ACCESSTOKEN','SERVICETOKEN','JWTTOKEN']
#additional keys defined by the developer in env_variables as a string.
env_keys = os.getenv('KEYS')
#convert the keys in env_var to list and add it to the sensitive_keys list.
if isinstance(env_keys,str):
    sensitive_keys.extend(env_keys.split(","))

"""This is a child class of RedactingFilter where redact method is overridden. It is necessary to change the functionality if the content(log_record) is a dictionary. The dictionary is traversed to find if any key is present in sensitive_keys list. If true, mask the value. """
class Custom_RedactFilter(RedactingFilter):

    "redact function is called from a filter function defined in the parent class. Refer to parent class RedactingFilter to understand the return of masked content to filter function. Once the filter function returns ture , log record is printed."
    def redact(self, content):
        # content is the log_record/log_message
        if content:
            if isinstance(content, dict):
                for k, v in content.items():
                    #check if key is present in sensitive keys, update value
                    if isinstance(v, str) and isinstance(k, str) and k.lower() in (key.lower() for key in sensitive_keys):
                        #if key is authorization, retain the token name. Ex. SSO , JWT
                        if k.lower() == 'authorization' :
                            content[k] = v.split(" ")[0] + ' ' +'[REDACTED]'
                        else:
                            content[k] = '[REDACTED]'
                    #for nested dictionary , call the function recursively.    
                    else:
                        content[k] = self.redact(v)

            elif isinstance(content, (list, tuple)):
                for i, v in enumerate(content):
                    content[i] = self.redact(v)

            else:
                #if content is str, substitute the matched pattern with the default mask.
                content = isinstance(content, str) and content or str(content)
                for pattern in self._patterns:
                    content = re.sub(pattern, self._default_mask, content)
        #returns the content to the parent class
        return content