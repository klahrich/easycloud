from setuptools import setup, find_packages

setup(
    name='Easy Cloud',
    url='https://github.com/klahrich/easycloud',
    author='Karim Lahrichi',
    author_email='klahrich@gmail.com',
    packages=find_packages(),
    install_requires=['pandas', 'google-cloud-bigquery', 'google-cloud-bigquery-storage'],
    version='0.1',
    license='MIT',
    description='A collection of easy to remember functions for manipulating data on cloud platforms',
)