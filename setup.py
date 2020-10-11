from os import path
from setuptools import setup, find_packages
import sys
import versioneer


here = path.abspath(path.dirname(__file__))

# with open(path.join(here, 'README.rst'), encoding='utf-8') as readme_file:
#     readme = readme_file.read()

with open(path.join(here, 'requirements.txt')) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    requirements = [line for line in requirements_file.read().splitlines()
                    if not line.startswith('#')]


setup(
    name='splash_ingest',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Different ingestors used in Splash",
    # long_description=readme,
    author="ALS Computing Group",
    author_email="dmcreynolds@lbl.gov",
    url="https://github.com/als-computing/splash-ingest",
    download_url="https://github.com/als-computing/splash-ingest/archive/v0.1.1.tar.gzz",
                   
    python_requires=">=3.7",
    packages=find_packages(exclude=["docs", "tests"]),
    entry_points={
        "databroker.handlers": [
            "MultiKeySlice = splash_ingest.handlers:MultiKeyHDF5DatasetSliceHandler"
        ]
    },
    include_package_data=True,
    package_data={
        'docstream': [
            # When adding files here, remember to update MANIFEST.in as well,
            # or else they will not be included in the distribution on PyPI!
            # 'path/to/data_file',
        ]
    },
    install_requires=requirements,
    license="BSD (3-clause)",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
    ],
)
