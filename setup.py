from distutils.core import setup

with open('README.me') as f:
    readme = f.read()

setup(
    name='distributed_kernel',
    version='0.1',
    packages=['distributed_kernel'],
    description='Distributed kernel for Jupyter',
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
