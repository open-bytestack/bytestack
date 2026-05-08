from setuptools import setup, find_packages

setup(
    name="bytestack-sdk",
    version="0.1.0",
    description="Bytestack SDK — local directory writer for the bytestack storage format",
    packages=find_packages(),
    install_requires=[
        "grpcio>=1.80.0",
        "protobuf>=6.0.0",
        "boto3>=1.34.0",
    ],
    python_requires=">=3.9",
)
