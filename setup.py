import setuptools

with open('README.md', "r") as f:
    readme = f.read()

setuptools.setup(
    name='distributed_notebook',
    version='0.2',
    packages=setuptools.find_packages(".", exclude=["distributed_notebook.demo"]),
    # packages=['distributed_notebook.demo', 'distributed_notebook.sync', 'distributed_notebook.kernel', 'distributed_notebook.smr'],
    description='Distributed notebook for Jupyter',
    long_description=readme,
    author='Benjamin Carver',
    author_email='bcarver2@gmu.edu',
    url='https://github.com/scusemua/distributed_notebook',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    entry_points={
        'jupyter_client.kernel_provisioners': [
            'gateway-provisioner = distributed_notebook.provisioner:GatewayProvisioner',
        ],
    },
    include_package_data=True,
)
