from setuptools import setup, find_packages

version = '1.0.0'

setup(name='pyrabbit2',
      version=version,
      description="A Pythonic interface to the RabbitMQ Management HTTP API",

      long_description="""\

Fork module to communicate the RabbitMQ HTTP Management API https://github.com/bkjones/pyrabbit

The main documentation lives at http://pyrabbit.readthedocs.org

There's no way to easily write programs against RabbitMQs management API
without resorting to some messy urllib boilerplate code involving HTTP
Basic authentication and parsing the JSON responses, etc. Pyrabbit
abstracts this away & provides an intuitive, easy way to work with the
data that lives inside of RabbitMQ, and manipulate the resources there.""",

      classifiers=[
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Natural Language :: English",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Software Development :: Libraries :: Python Modules",
          ],
      keywords='python http amqp rabbit rabbitmq management',
      install_requires = ['requests'],
      author='Brian K. Jones, Yuri Bukatkin',
      author_email='bkjones@gmail.com, windowod@gmail.com',
      url='https://github.com/deslum/pyrabbit2',
      download_url='https://github.com/deslum/pyrabbit2/archive/master.zip',
      license='MIT',
      packages=find_packages(exclude='tests'),
      include_package_data=False,
      zip_safe=False
      )
