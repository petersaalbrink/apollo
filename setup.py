from setuptools import setup

__version__ = None
exec(open("common/etc/version.py").read())  # noqa

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    pkgs = [p.strip("\r\n") for p in f.readlines()]

setup(
    name="common_classes_mx",
    version=__version__,
    author="Peter Saalbrink",
    author_email="psaalbrink@matrixiangroup.com",
    keywords="common classes matrixian group mx",
    description="Common classes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/matrixiangroup_dev/common_classes_mx",
    packages=[
        "common",
        "common.api",
        "common.connectors",
        "common.customer_communication",
    ],
    install_requires=pkgs,
    package_data={"": ["certificates/*.pem", "etc/*", "etc/.env"]},
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
