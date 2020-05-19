from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    # Application type:
    name="file-compare",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="gongzy",
    author_email="gongzy@iata.org",

    # Packages
    packages=["file_compare"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://git.iata-asd.org/IBSPS/comparetools",
    project_urls={
        "Documentation": "https://git.iata-asd.org/IBSPS/comparetools/tree/master/document",
        "Source Code": "https://git.iata-asd.org/IBSPS/comparetools/tree/master/source/File_compare",
    },

    #
    description="IBSPs HOT and CSI file comparison tool.",
    # long_description=long_description,
    # long_description_content_type="text/markdown",

    # Dependent packages (distributions)
    install_requires=[
        "cx_Oracle", "lxml"
    ],

    classifiers=[
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",

    ],

    python_requires='>=3.6',
)
