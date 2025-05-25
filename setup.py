from setuptools import setup, find_packages

setup(
    name="elia_opendata",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "urllib3>=1.26.0",
    ],
    author="WattsToAnalyze",
    author_email="",  # Add your email
    description="A Python package for accessing the Elia Open Data Portal",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/WattsToAnalyze/elia-opendata",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
)