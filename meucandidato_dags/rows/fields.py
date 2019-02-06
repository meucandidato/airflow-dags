import datetime
import rows
import six


def as_string(value):
    if isinstance(value, six.binary_type):
        raise ValueError("Binary is not supported")
    elif isinstance(value, six.text_type):
        return value
    else:
        return six.text_type(value)


class PtBrDateField(rows.fields.DateField):
    INPUT_FORMAT = '%d/%m/%Y'


class PtBrDateTimeMinField(rows.fields.DateField):
    TYPE = (datetime.datetime,)
    INPUT_FORMAT = '%d/%m/%Y'

    @classmethod
    def deserialize(cls, value, *args, **kwargs):
        if value is None or isinstance(value, cls.TYPE):
            return value

        value = as_string(value)

        dt_object = datetime.datetime.strptime(value, cls.INPUT_FORMAT)
        return dt_object


class PtBrDateTimeField(rows.fields.DatetimeField):
    INPUT_FORMAT = '%d/%m/%Y %H:%M:%S'