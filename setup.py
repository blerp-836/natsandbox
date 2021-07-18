import os
from setuptools import setup,find_packages
import argparse

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()
if __name__ == "__main__":
 
    setup(
        name = "sandbox",
        version = "0.0.2",
        author = "Natalia Theodora",
        author_email = "nattheodora@gmail.com",
        description = ("sample lambda architecture"),
        license = "MIT",
        keywords = "example azure lambda architecutre",
        url = "https://github.com/blerp-836/sandbox.git",
        packages=find_packages(where='.',exclude=['*/main*',]),
        package_data={'utilities':['config/*.ini']},
        long_description=read('README.md'),
        classifiers=[
            "Development Status :: 3-Alpha",
            "Topic :: azure sandbox",
            "License :: MIT License",
        ],
    )