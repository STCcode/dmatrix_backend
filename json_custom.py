# json_custom.py
import json
from decimal import Decimal
from datetime import date, datetime, time

try:
    from flask.json.provider import DefaultJSONProvider
    HAS_PROVIDER = True
except Exception:
    HAS_PROVIDER = False


def _convert_decimal(x: Decimal):
    if x == x.to_integral_value():
        return int(x)
    return float(x)


def _default_encoder(obj):
    if isinstance(obj, Decimal):
        return _convert_decimal(obj)
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


if HAS_PROVIDER:  # Flask ≥ 2.3 / 3.x
    from flask.json.provider import DefaultJSONProvider

    class CustomJSONProvider(DefaultJSONProvider):
        def default(self, o):
            try:
                return _default_encoder(o)
            except TypeError:
                return super().default(o)

        def dumps(self, obj, **kwargs):
            kwargs.setdefault("default", self.default)
            kwargs.setdefault("sort_keys", False)
            return super().dumps(obj, **kwargs)


class CustomJSONEncoder(json.JSONEncoder):  # Flask ≤ 2.2
    def default(self, obj):
        try:
            return _default_encoder(obj)
        except TypeError:
            return super().default(obj)


def init_json(app):
    if HAS_PROVIDER:
        app.json_provider_class = CustomJSONProvider
        app.json = CustomJSONProvider(app)
    else:
        app.json_encoder = CustomJSONEncoder
