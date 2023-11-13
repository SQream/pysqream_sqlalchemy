from setuptools import setup


setup_params = dict(
    
    name =             'pysqream_sqlalchemy',
    version =          '0.9',
    description =      'SQLAlchemy dialect for SQreamDB', 
    long_description = open("README.rst", "r").read() + '\n\n',
    url=               "https://github.com/SQream/pysqream_sqlalchemy",
    
    author =           'SQream',
    author_email =     'info@sqream.com',
    
    classifiers =      [
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    keywords = 'database sqlalchemy sqream sqreamdb',

    python_requires =  '>=3.9',
    
    install_requires = ['sqlalchemy==1.4.46',
                        'pysqream==3.2.5',
                        'setuptools>=57.4.0',
                        'pandas>=1.5.3',
                        'numpy>=1.20',
                        'alembic>=1.10.2'],
    
    packages         = ['pysqream_sqlalchemy'], 
    
    entry_points =     {'sqlalchemy.dialects': 
        ['sqream = pysqream_sqlalchemy.dialect:SqreamDialect']
    },
    # sqream://sqream:sqream@localhost/master
)


if __name__ == '__main__':
    setup(**setup_params)
