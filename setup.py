from setuptools import setup, find_packages

setup(
    name="botolib",
    version="0.0.3",
    author="Jun Ke",
    author_email="kejun91@gmail.com",
    description="A boto lib that enhances some aws service clients",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kejun91/botolib",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)