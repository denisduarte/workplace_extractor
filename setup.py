from distutils.core import setup
setup(
    name='workplace_extractor',
    packages=['workplace_extractor'],
    version='0.6',
    license='MIT',
    description='Extract posts created in a corporate Workplace by Facebook installation using the Graph API',
    author='Denis Duarte',
    author_email='den.duarte@gmail.com',
    url='https://github.com/denisduarte/workplace_extractor',
    download_url='https://github.com/denisduarte/workplace_extractor/archive/refs/tags/v_06.tar.gz',
    keywords=['extraction', 'posts', 'workplace'],
    install_requires=[
        'numpy',
        'pandas',
        'aiohttp',
        'asyncio'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.9',
    ],
)
