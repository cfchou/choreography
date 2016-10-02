# vim:fileencoding=utf-8
from setuptools import setup, find_packages
from codecs import open
from os import path

__version__ = '0.0.1'

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# get the dependencies and installs
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' not in x]

setup(
    name='choreography',
    version=__version__,
    description='Extensible MQTT benchmark/simulation framework',
    long_description=long_description,
    url='https://github.com/cfchou/choreography',
    download_url='https://github.com/cfchou/choreography/tarball/' + __version__,
    license='BSD',
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'Programming Language :: Python :: 3',
    ],
    keywords='mqtt, benchmark',
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    author='Chifeng Chou',
    install_requires=install_requires,
    dependency_links=dependency_links,
    author_email='cfchou@gmail.com',
    entry_points={
        #'console_scripts': ['choreograph=choreography.scripts.choreograph:main'],
        'choreography.launcher_plugins': [
            'LinearLauncher = choreography.launcher_plugins.linear:LinearLauncher'
        ]
    }
)
