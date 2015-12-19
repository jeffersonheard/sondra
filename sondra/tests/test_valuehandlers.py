from sondra.collection import DateTime, Geometry, Now
from datetime import datetime
import rethinkdb as r


def test_datetime():
    dt = DateTime()
    now = datetime.utcnow()
    now_seconds = datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
    now_str = now_seconds.isoformat()
    now_rdb = r.time(now.year, now.month, now.day, now.hour, now.minute, now.second, 'Z')

    assert isinstance(dt.to_json_repr(now_seconds), str)
    assert dt.to_json_repr(now_rdb) == now_str
    assert dt.to_json_repr(now_seconds) == now_str
    assert dt.to_json_repr(dt.to_python_repr(now_str))
    assert dt.to_json_repr(dt.to_rql_repr(now_str))


