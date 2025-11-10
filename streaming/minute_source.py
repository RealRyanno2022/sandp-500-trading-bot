# streaming/minute_source.py
import asyncio
from collections import deque

async def minute_bar_stream(source_seconds):
    """
    source_seconds: async generator yielding dict(ts,o,h,l,c,v) at 1-second.
    Rolls to 1-minute bars.
    """
    buf = deque()
    cur_min = None
    async for s in source_seconds():
        t = pd.to_datetime(s["ts"], utc=True)
        minute = t.replace(second=0, microsecond=0)
        if cur_min is None: cur_min = minute
        if minute != cur_min and buf:
            o = buf[0]["o"]; h = max(x["h"] for x in buf)
            l = min(x["l"] for x in buf); c = buf[-1]["c"]
            v = sum(x["v"] for x in buf)
            yield {"ts": cur_min.isoformat(), "o":o,"h":h,"l":l,"c":c,"v":v}
            buf.clear(); cur_min = minute
        buf.append(s)
