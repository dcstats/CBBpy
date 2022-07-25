from setuptools import setup, find_packages


setup(
    name='CBBpy',
    version='0.3',
    license='MIT',
    author="Daniel Cowan",
    author_email='dnlcowan37@gmail.com',
    description='A Python-based web scraper for NCAA basketball.',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/dcstats/CBBpy',
    keywords='college basketball scraper',
    install_requires=[
        'pandas',
        'numpy',
        'python-dateutil',
        'pytz',
        'tqdm'
      ],

)