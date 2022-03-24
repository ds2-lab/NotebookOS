import setuptools

with open('README.md', "r") as f:
    readme = f.read()

setuptools.setup(
    name='distributed-notebook',
    version='0.2',
    packages=setuptools.find_packages(".", exclude=["distributed-notebook.demo"]),
    # packages=['distributed-notebook.demo', 'distributed-notebook.sync', 'distributed-notebook.kernel', 'distributed-notebook.smr'],
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
    include_package_data=True,
)
