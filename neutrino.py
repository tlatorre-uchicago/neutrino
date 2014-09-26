# Copyright (c) 2014, Anthony LaTorre
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
from collections import namedtuple
import re
import msgpack
from redis import StrictRedis
import struct
import itertools
from functools import partial
from jinja2 import Template

def unpack_array(fmt, array_string):
    "Unpack a binary array"
    # unpack_array('f','\x00'*8) --> [0.0, 0.0]
    size = struct.calcsize(fmt)
    for i in range(0,len(array_string),size):
        yield struct.unpack(fmt,array_string[i:i+size])

def pad(x, size):
    """
    Pad `x` with null bytes to the right. If `x` is None, returns
    '\x00'*size.
    """
    # pad('hello', 10) --> 'hello\x00\x00\x00\x00\x00'
    if x:
        return x.ljust(size,'\x00')
    else:
        return '\x00'*size

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)]*n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

# convert python struct format characters -> lua
# struct format characters
# need to specify byte sizes explicitly in lua because
# it is native size by default
# python's struct.unpack guarantees standard sizes if
# endianness is explicitly included in the format string.
LUA_FORMAT = {
    'c' : 'c',
    'b' : 'i1',
    'B' : 'I1',
    'h' : 'i2',
    'H' : 'I2',
    'i' : 'i4',
    'I' : 'I4',
    'l' : 'i4',
    'L' : 'i4',
    'q' : 'i8',
    'Q' : 'I8',
    'f' : 'f',
    'd' : 'd'
}

LUA_STRING = """
-- one byte to keep track of whether it's been set
local format = '<' .. ARGV[1] .. 'I{{ bytes }}'
local index = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local args = cmsgpack.unpack(ARGV[4])

for i, key in ipairs(KEYS) do
    local value = args[i]
    local size = struct.size(format)
    local start = index*size

    local bytes = redis.call('GETRANGE',key,start,start+size)
    if bytes:len() < size then
        -- just set it
        redis.call('SETRANGE',key,start,struct.pack(format,value,1))
    else
        local prev, count = struct.unpack(format,bytes)
        if count == 0 then
            -- just set it
            redis.call('SETRANGE',key,start,struct.pack(format,value,1))
        else
            local next = {{ next }}
            -- need to make sure that count doesn't wrap over to 0
            -- otherwise the next call will overwrite the current
            -- value.
            if count < {{ 2**(8*bytes) - 1 }} then count = count + 1 end
            redis.call('SETRANGE',key,start,struct.pack(format,next,count))
        end
    end
    redis.call('EXPIRE',key,ttl)
end
"""

LUA_TEMPLATE = Template(LUA_STRING)

LUA_METHODS = {
    'sum'  : {'bytes': 1, 'next': 'prev + value'},
    'max'  : {'bytes': 1, 'next': 'math.max(prev,value)'},
    'min'  : {'bytes': 1, 'next': 'math.min(prev,value)'},
    'avg'  : {'bytes': 4, 'next': 'prev + value'},
    'last' : {'bytes': 1, 'next': 'value'},
    'rate' : {'bytes': 1, 'next': 'prev + value'}
}

Rule = namedtuple('Rule',['pattern','fmt','step','chunk','ttl','method'])

class TimeSeries(object):
    """
    Returns a new TimeSeries object.

    Example:

        >>> ts = TimeSeries(Redis(port=6380))
        >>> ts.add_rule('spam.*','f',1,100,1000,'sum')
        >>> ts.add_rule('blah.*','f',1,100,1000,'avg')
        >>> ts.update({'spam.1': 10, 'blah.1': 10},0)
        >>> ts.update({'spam.1': 20, 'blah.1': 20},0)
        >>> ts.update({'spam.1': 10, 'blah.1': 10},1)
        >>> ts.fetch_range('spam.1',0,10,1)
        [30.0, 10.0, None, None, None, None, None, None, None, None]
        >>> ts.fetch_range('blah.1',0,10,1)
        [15.0, 10.0, None, None, None, None, None, None, None, None]
    """
    def __init__(self, redis=None):
        self.rules = []
        if redis is None:
            self.redis = StrictRedis()
        else:
            self.redis = redis
        self._register_scripts()

    def _register_scripts(self):
        self.scripts = {}
        for method, kwargs in LUA_METHODS.iteritems():
            script = LUA_TEMPLATE.render(**kwargs)
            self.scripts[method] = self.redis.register_script(script)

    def add_rule(self, pattern, fmt, step, chunk, ttl, method):
        """
        Add a timeseries rule. `pattern` should be a python regular
        expression (see https://docs.python.org/2/howto/regex.html).
        Any key later added which matches this expression will be saved
        according to the rule.
        
        `fmt` is a single character for the data type. See
        https://docs.python.org/3/library/struct.html#format-characters
        for a list of possible format characters.

        Time series data is organized into chunks which are stored in a
        single redis key. This allows data to expire after a certain
        period of time. `chunk` controls how many time series points to
        store in a single chunk and `ttl` is how many seconds to keep that
        chunk alive.

        `method` specifies how multiple data points received in a
        single time step are handled. `method` may be 'avg', 'min',
        'max', 'last', 'sum', or 'rate'.
        """
        if method not in LUA_METHODS:
            raise ValueError("unknown method %s" % method)
        if fmt not in LUA_FORMAT:
            raise ValueError("unknown format %s" % fmt)
        p = re.compile(pattern)
        rule = Rule(p, fmt, step, chunk, ttl, method)
        self.rules.append(rule)

    def load(self, line):
        """
        Load rule from string `line`. line should have the format:
            pattern format step chunk ttl method
        all separated by spaces. If the regular expression has
        spaces you can enclose it in parentheses.
        """
        self.add_rule(*shlex.split(line))

    def fetch_range(self, name, start, stop, step):
        """
        Fetch a timeseries from start to stop in steps of step.
        start and stop should be unix timestamps, and step should
        be in seconds.
        """
        # find rule with the greatest step which matches `name`
        # and has rule.step <= `step`
        for rule in reversed(sorted(self.rules,key=lambda x: x.step)):
            if rule.pattern.match(name) and rule.step <= step:
                break
        else:
            msg = 'No rule found for %s with step < %i' % (name, step)
            raise RuntimeError(msg)

        index_start = start - start % rule.chunk
        offset_start = (start % rule.chunk)//rule.step
        index_stop = stop - stop % rule.chunk
        offset_stop = (stop % rule.chunk)//rule.step

        if rule.method == 'avg':
            fmt = rule.fmt + 'I'
        else:
            fmt = rule.fmt + 'B'

        chunk_size = struct.calcsize(fmt)*rule.chunk

        p = self.redis.pipeline()
        for i in range(index_start,index_stop+1,rule.chunk):
            p.get('ts:%s:%i:%i:%s' % (rule.fmt,rule.step,i,name))
        chunks = map(partial(pad,size=chunk_size),p.execute())

        select = [offset_start + (start % rule.step + i)//rule.step \
                  for i in range(0,stop-start,step)]

        series = list(unpack_array('<' + fmt,''.join(chunks)))

        # filter selected elements
        series = [series[i] for i in select]

        if rule.method == 'avg':
            # divide sum by count
            return [x/n if n else None for x, n in series]
        elif rule.method == 'rate':
            # divide sum by time series step to get a rate in Hz
            return [x/rule.step if n else None for x, n in series]
        else:
            return [x if n else None for x, n in series]

    def update(self, data, unix_time):
        """
        Update multiple timeseries. `data` should be a dictionary mapping
        timeseries names to data.
        """
        for rule in self.rules:
            index = unix_time - unix_time % rule.chunk
            prefix = 'ts:%s:%i:%i:%%s' % (rule.fmt,rule.step,index)
            match = dict((prefix % k,data[k]) for k in data
                         if rule.pattern.match(k))
            offset = (unix_time % rule.chunk)//rule.step
            func = self.scripts[rule.method]
            fmt = LUA_FORMAT[rule.fmt]
            k, v = zip(*match.iteritems())
            func(keys=k, args=[fmt,offset,rule.ttl,msgpack.packb(v)])

