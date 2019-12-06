import os, glob
from setuptools import setup

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

setup(
    name="s3mi",
    version="0.9",
    url='https://github.com/chanzuckerberg/s3mi',
    license=open("LICENSE").readline().strip(),
    author='S3MI contributors',
    author_email='bdimitrov@chanzuckerberg.com',
    description='Transfer big files fast between AWS S3 and EC2.  Pronounced semi.',
    long_description=open('README.md').read(),
    install_requires=install_requires,
    extras_require={},
    packages=None,
    package_dir=None,
    scripts=glob.glob('scripts/*'),
    data_files=None,
    platforms=['MacOS X', 'Posix'],
    zip_safe=False,
    test_suite=None,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
