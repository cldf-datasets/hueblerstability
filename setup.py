from setuptools import setup


setup(
    name='cldfbench_hueblerstability',
    py_modules=['cldfbench_hueblerstability'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'hueblerstability=cldfbench_hueblerstability:Dataset',
        ]
    },
    install_requires=[
        'pyglottolog>=3.6',
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
