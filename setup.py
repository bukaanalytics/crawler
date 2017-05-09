from setuptools import setup

config = {
    'name': 'crawler',
    'description': 'Crawls Bukalapak seller stats using Bukalapak API',
    'author': 'bukaanalytics',
    'url': 'https://github.com/bukaanalytics/crawler',
    'version': '0.0.1',
    'install_requires': ['pymongo', 'pytz', 'requests'],
    'packages': [],
    'scripts': [],
    'license':'MIT'
}

setup(**config)