from distutils.core import setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='distributed-notebook',
    version='0.1',
    packages=['distributed-notebook.demo', 'distributed-notebook.sync', 'distributed-notebook.kernel'],
    description='Distributed notebook for Jupyter',
    long_description=readme,
    author='Tianium',
    author_email='jzhang33@gmu.edu',
    url='https://github.com/zhangjyr/distributed_notebook',
    install_requires=[
        'jupyter_client', 'IPython', 'ipykernel'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
)
