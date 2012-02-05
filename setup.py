from setuptools import setup
from setuptools import Extension

mod = Extension(
        'boost_queue',
        sources=['boost_queue.cpp'],
        libraries=['boost_thread', 'boost_date_time'],
        extra_compile_args=["-O2"],
        depends=['concurrent_queue.hpp']
        )

setup(
    name='boost_queue',
    version='0.1',
    description="Queue using boost's locking API",
    long_description=open('README.rst').read(),
    classifiers=[
        'Programming Language :: C++',
        'Intended Audience :: Developers',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        ],
    keywords='queue boost',
    author='Stephan Hofmockel',
    author_email="Use the github issues",
    url='https://github.com/stephan-hof/boost_queue',
    license='boost',
    packages=['tests'],
    ext_modules=[mod]
)
