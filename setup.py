from setuptools import find_packages, setup

__version__ = None
exec(open("common/etc/version.py").read())  # noqa

pkgs = [p for p in (p.strip("\n") for p in open("requirements.txt").readlines()) if p]
extras = {"all": [p for p in pkgs if not p.startswith("# ")]}
extra = ""
for pkg in pkgs:
    if pkg.startswith("# "):
        extra = pkg[2:]
        extras[extra] = []
    else:
        extras[extra].append(pkg)

setup(
    name="common_classes_mx",
    version=__version__,
    author="Peter Saalbrink",
    author_email="psaalbrink@matrixiangroup.com",
    keywords="common classes matrixian group mx",
    description="Common classes",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/matrixiangroup_dev/common_classes_mx",
    packages=find_packages(),
    extras_require=extras,
    package_data={"": ["customer_communication/*", "etc/*", "etc/.env"]},
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7,<3.11",
)
