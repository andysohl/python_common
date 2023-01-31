from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

# Refer to https://github.com/pypa/sampleproject

setup(
    name='analytics-common',
    version='1.0.3',
    description='An analytics support library for Tricor',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://dev.azure.com/tricorglobal/_git/Data%20analytics',
    author='Tricor ACoE',
    author_email='TricorSSC-ACOE@my.tricorglobal.com',
    classifiers=[  # Optional
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only'
    ],
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.7, <4',
    install_requires=[
        'pyspark',
        'numpy', 'pandas',
        'xxhash', 'httpx[http2]',
        'openpyxl>=3.0.10', 'msoffcrypto-tool>=5.0.0'
    ],
)

