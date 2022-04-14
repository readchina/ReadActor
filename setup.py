import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ReadChinaLookup",
    version="1.0.0",
    author="Qin Gu",
    author_email="guqin7@gmail.com",
    description="A lookup tool for ReadChina project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/readchina/WikidataLookup",
    project_urls={
        "Bug Tracker": "https://github.com/pypa/ReadChinaLookup/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
)
