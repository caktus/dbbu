from setuptools import setup, find_packages

setup(
    name="dbbu",
    version="0.1.1",
    packages=find_packages(),
    scripts=[
        'scripts/dbbu-run.py',
    ],
)
