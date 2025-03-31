from setuptools import setup


setup_params = dict(
    name='pysqream_sqlalchemy',
    version='1.4',
    description='SQLAlchemy dialect for SQreamDB',
    long_description=open("README.rst", "r").read() + '\n\n',
    url="https://github.com/SQream/pysqream_sqlalchemy",
    
    author='SQream',
    author_email='info@sqream.com',
    
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    keywords='database sqlalchemy sqream sqreamdb',

    python_requires='>=3.9',
    
    install_requires=['sqlalchemy==2.0.27',
                      'pysqream>=5.1.0',
                      'setuptools>=57.4.0',
                      'pandas==2.2.2',
                      'numpy==1.26.4',
                      'alembic>=1.10.2'],
    
    packages=['pysqream_sqlalchemy'],
    
    entry_points={'sqlalchemy.dialects': ['sqream = pysqream_sqlalchemy.dialect:SqreamDialect']},
    # sqream://sqream:sqream@localhost/master
)


if __name__ == '__main__':
    setup(**setup_params)
