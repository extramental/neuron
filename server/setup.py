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
      entry_points="""\
      [console_scripts]
      neuron = neuron:main
      """,
      )
