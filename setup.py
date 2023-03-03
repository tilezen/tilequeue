import os.path

from setuptools import find_packages
from setuptools import setup

version_path = os.path.join(os.path.dirname(__file__), 'VERSION')
with open(version_path) as fh:
    version = fh.read().strip()

setup(name='tilequeue',
      version=version,
      description='Queue operations to manage the processes surrounding tile '
                  'rendering.',
      long_description=open('README.md').read(),
      classifiers=[
          # strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Internet :: WWW/HTTP :: Site Management',
          'Topic :: Utilities',
      ],
      keywords='aws queue s3 sqs tile map',
      author='Robert Marianski, Mapzen',
      author_email='rob@mapzen.com',
      url='https://github.com/mapzen/tilequeue',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'boto',
          'boto3',
          'edtf',
          'enum34',
          'hiredis',
          'Jinja2>=2.10.1',
          'mapbox-vector-tile',
          'ModestMaps',
          'protobuf',
          'psycopg2',
          'pyproj>=2.1.0',
          'python-dateutil',
          'PyYAML',
          'raw_tiles',
          'redis',
          'requests',
          'Shapely',
          'statsd',
          'ujson',
          'zope.dottedname',
      ],
      test_suite='tests',
      tests_require=[
          'mock',
          'httptestserver'
      ],
      entry_points=dict(
          console_scripts=[
              'tilequeue = tilequeue.command:tilequeue_main',
          ]
      )
      )
