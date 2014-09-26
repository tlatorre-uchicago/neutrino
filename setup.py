from distutils.core import setup

setup(name="neutrino",
      version="0.1",
      description="High Performance Redis Timeseries",
      author="Anthony LaTorre",
      py_modules=['neutrino'],
      requires=['msgpack-python',
                'redis',
                'jinja2'],
     )
