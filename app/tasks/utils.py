import re


class ValueMaskUtility(object):

    MASK_KEYS = {"user_token": "uuid", "apitoken": "uuid", "to_address": "email", "email": "email"}

    @classmethod
    def mask_value(cls, name: str, value: str):
        if name and name in cls.MASK_KEYS:
            mask_type = cls.MASK_KEYS[name]
            if mask_type == "uuid":
                return cls.mask_uuid(value)
            elif mask_type == "email":
                return cls.mask_email(value)
        return value
    @classmethod
    def mask_uuid(cls, value):
        if len(value) < 2:
            return value
        if len(value) > 7:
            replaced = re.sub('[^-]', '*', value)
            replaced = value[:3] + replaced[3:-3] + value[-3:]
        else:
            replaced = value[:1] + replaced[1:-1] + value[-1:]
            
        return replaced
    
    @classmethod
    def mask_email(cls, value):
        if not value:
            return ""
        data = value.split("@")
        if len(data) > 1:
            head = data[0]
            if len(head) > 3:
                replaced = re.sub('[\w]', '*', head)
                replaced = head[0] + replaced[1:-1] + head[-1:]
            else:
                if len(head) > 1:
                    replaced = head[0] + replaced[1:]
                else:
                    replaced = "*"
            data[0] = replaced
            email = "@".join(data)
            return email
        
        replaced = re.sub('[\w]', '*', value)
        if len(replaced) > 2:
            replaced = value[0] + replaced[1:-1] + value[-1:]
        return replaced
    
