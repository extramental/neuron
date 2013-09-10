import os

from setuptools import setup, find_packages

requires = [
    "tornado",
    "redis",
    "sockjs-tornado"
    ]

setup(name="neuron",
      version="0.0",
      description="neuron",
      author="",
      author_email="",
      url="",
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite="neuron",
      install_requires=requires,
      dependency_links=["http://github.com/mrjoes/sockjs-tornado/tarball/master#egg=sockjs-tornado-1.0.0"],
      entry_points="""\
      [console_scripts]
      neuron = neuron:main
      """
      )
