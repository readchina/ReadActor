from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ReadActor",
    version="1.0.1-beta",
    author="Qin Gu",
    author_email="guqin7@gmail.com",
    description="A lookup tool for ReadChina project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/readchina/ReadActor",
    project_urls={
        "Bug Tracker": "https://github.com/pypa/ReadActor/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
    ],
    entry_points={
        "console_scripts": [
            "readactor = src.scripts.readactor:cli",
        ],
    },
)
