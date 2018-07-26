from setuptools import setup, find_packages

setup(
    name='stardust',
    version='0.0.1',
    description='An algorithmic trading engine',
    url='https://github.com/hard-codr/stardust.git',
    license='Apache',
    author='hardcodr',
    author_email='code@hardcodr.com',
    include_package_data=True,
    packages=find_packages(),
    classifiers=[
        'Development Status :: 0 - Alpha/unstable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'aiohttp', 'aiosqlite', 'numpy', 'PyYAML', 'TA-Lib',
    ],
    dependency_links=[
        'git+https://github.com/hard-codr/sirius',
    ],
)
