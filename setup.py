import setuptools

setuptools.setup(
    name="wb_poc",
    version="0.0.1",
    author="Gufran Pathan",
    author_email="author@example.com",
    description="Package for parsing pdf to textg",
    # long_description=long_description,
    long_description_content_type="text/markdown",
    #url="https://github.com/pypa/sampleproject",
    #packages=setuptools.find_packages(),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6',
)