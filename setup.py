from setuptools import setup


setup(
    name='pdpart',
    version='0.2.0',
    description='partition pandas DataFrames by key',
    url='http://github.com/mossadnik/pdpart',
    author='Matthias Ossadnik',
    author_email='ossadnik.matthias@gmail.com',
    license='MIT',
    setup_requires=['pytest-runner'],
    install_requires=['pandas'],
    tests_require=['pytest'],
    packages=['pdpart'],
    zip_safe=False,
)
