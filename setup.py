import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="common_classes_mx",
    version="0.0.1",
    author="Peter Saalbrink",
    author_email="psaalbrink@matrixiangroup.com",
    keywords="common classes matrixian group mx",
    description="Common classes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/matrixiangroup_dev/common_classes_mx",
    packages=setuptools.find_packages(),
    install_requires=[
        "mysql-connector-python",
        "pymongo",
        "text_unidecode",
        "elasticsearch",
        "tqdm",
        "pandas",
        "numpy",
        "matplotlib",
        "requests",
        "pycountry",
    ],
    package_data={"common": ["certificates/*.pem"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
