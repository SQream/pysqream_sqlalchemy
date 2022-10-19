from setuptools import setup


setup_params = dict(
    
    name =             'pysqream_sqlalchemy',
    version =          '0.7',
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

    python_requires =  '>=3.6',
    
    install_requires = ['sqlalchemy>=1.3.18',
                        'pysqream>=3.1.8',
                        'setuptools==57.4.0',
                        'pytest==6.2.3',
                        'pudb==2022.1.2',
                        'pandas==1.1.5',
                        'numpy==1.19.5',
                        'alembic==1.5.8'],
    
    packages         = ['pysqream_sqlalchemy'], 
    
    entry_points =     {'sqlalchemy.dialects': 
        ['sqream = pysqream_sqlalchemy.dialect:SqreamDialect']
    },
    # sqream://sqream:sqream@localhost/master
)


if __name__ == '__main__':
    setup(**setup_params)
