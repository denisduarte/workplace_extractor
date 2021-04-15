from distutils.core import setup
setup(
    name = 'workplace_extractor',
    packages = ['workplace_extractor'],
    version = '0.1',
    license='MIT',
    description = 'Package to extract posts created in a corporate Workplace by Facebook installation using the Graph API',
    author = 'Denis Duarte',
    author_email = 'den.duarte@gmail.com',
    url = 'https://github.com/denisduarte/workplace_extractor',
    download_url = 'https://github.com/denisduarte/workplace_extractor/archive/refs/tags/v_01.tar.gz',
    keywords = ['extraction', 'posts', 'workplace'],
    install_requires=[            # I get to this in a second
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