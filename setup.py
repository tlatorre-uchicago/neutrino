from setuptools import setup

setup(name="neutrino",
      version="0.2",
      description="High Performance Redis Timeseries",
      author="Anthony LaTorre",
      url="https://github.com/tlatorre-uchicago/neutrino",
      keywords=["redis", "timeseries"],
      py_modules=['neutrino'],
      install_requires=['msgpack_python','redis','jinja2'],
     )
