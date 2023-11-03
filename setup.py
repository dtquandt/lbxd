from setuptools import setup, find_packages

setup(
   name='lbxd',
   version='0.1',
   packages=find_packages(),
   description='Wrapper around the letterboxd library which is itself a wrapper around the Letterboxd API ',
   author='Daniel Quandt',
   author_email='danieltquandt@gmail.com',
   install_requires=['pandas', 'pybase62', 'letterboxd', 'python-dotenv'], #external packages as dependencies
)