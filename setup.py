from setuptools import setup


setup_params = dict(
    
    name =             'pysqream_sqlalchemy',
    version =          '0.4',
    description =      'SQLAlchemy dialect for SQreamDB', 
    long_description = open("README.rst", "r").read() + '\n\n',
    url=               "https://github.com/SQream/pysqream_sqlalchemy",
    
    author =           'EliYk',
    author_email =     'eliy@sqreamtech.com',
    
    classifiers =      [
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    keywords = 'database sqlalchemy sqream sqreamdb',

    python_requires =  '>=3.6',
    
    install_requires = ['sqlalchemy>=1.3.18',
                       'pysqream>=3.0.3'],
    
    packages         = ['pysqream_sqlalchemy'], 
    
    entry_points =     {'sqlalchemy.dialects': 
        ['sqream = pysqream_sqlalchemy.dialect:SqreamDialect']
    },
    # sqream://sqream:sqream@localhost/master
)


if __name__ == '__main__':
    setup(**setup_params)
