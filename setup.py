from setuptools import setup

setup(
   name='lbxd',
   version='0.1',
   description='Wrapper around the letterboxd library which is itself a wrapper around the Letterboxd API ',
   author='Daniel Quandt',
   author_email='danieltquandt@gmail.com',
   packages=['lbxd'],  #same as name
   install_requires=['pandas', 'base62', 'letterboxd', 'python-dotenv'], #external packages as dependencies
)