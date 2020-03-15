from setuptools import setup, find_packages

setup(
    name='easycloud',
    url='https://github.com/klahrich/easycloud',
    author='Karim Lahrichi',
    author_email='klahrich@gmail.com',
    packages=find_packages(),
    install_requires=['pandas', 'google-cloud-storage', 'google-cloud-bigquery', 'google-cloud-bigquery-storage', 'pyaml'],
    version='0.1',
    license='MIT',
    description='A collection of easy to remember functions to manipulate data on cloud platforms',
    entry_points='''
        [console_scripts]
        bigquery=easycloud.gcp:bigquery
    '''
)
