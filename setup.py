import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="common",
    version="0.0.1",
    author="Peter Saalbrink",
    author_email="psaalbrink@matrixiangroup.com",
    keywords="common",
    description="Common classes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/matrixiangroup_dev/common_classes_peter",
    packages=setuptools.find_packages(),
    package_data={"": [
        "client-cert.pem",
        "client-key.pem",
        "server-ca.pem",
        "achternamen_spreiding_clean.csv",
        "titelatuur.csv",
        "voornamen_spreiding_clean.csv"
    ]},
    data_files=[
        ("certificates", [
            "certificates/client-cert.pem",
            "certificates/client-key.pem",
            "certificates/server-ca.pem"
        ]),
        ("data", [
            "data/achternamen_spreiding_clean.csv",
            "data/titelatuur.csv",
            "data/voornamen_spreiding_clean.csv"
        ])],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
