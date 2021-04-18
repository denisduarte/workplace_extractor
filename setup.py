from distutils.core import setup
setup(
    name='workplace_extractor',
    packages=['workplace_extractor', 'workplace_extractor.Nodes', 'workplace_extractor.Extractors'],
    version='0.6.3',
    license='MIT',
    description='Extract posts created in a corporate Workplace by Facebook installation using the Graph API',
    author='Denis Duarte',
    author_email='den.duarte@gmail.com',
    url='https://github.com/denisduarte/workplace_extractor',
    download_url='https://github.com/denisduarte/workplace_extractor/archive/refs/tags/v_0.6.3.tar.gz',
    keywords=['extraction', 'posts', 'workplace'],
    install_requires=[
        'numpy>=1.20',
        'pandas>=1.2',
        'aiohttp>=3.7',
        'asyncio>=3.4'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
