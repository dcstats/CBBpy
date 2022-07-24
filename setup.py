from setuptools import setup, find_packages


setup(
    name='CBBpy',
    version='0.1',
    license='MIT',
    author="Daniel Cowan",
    author_email='dnlcowan37@gmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/dcstats/CBBpy',
    keywords='college basketball scraper',
    install_requires=[
        'pandas',
        'numpy',
        'html',
        'datetime',
        'dateutil',
        'pytz',
        'tqdm',
        're',
        'time',
        'logging'
      ],

)