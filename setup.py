from setuptools import find_packages, setup

import versioneer

install_requires = open("requirements.txt").read().strip().split("\n")
with open("README.md") as f:
    long_description = f.read()

setup(
    name="afar",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Run code on a Dask cluster via a context manager or IPython magic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Erik Welch",
    author_email="erik.n.welch@gmail.com",
    url="https://github.com/eriknw/afar",
    packages=find_packages(),
    license="BSD",
    python_requires=">=3.7",
    setup_requires=[],
    install_requires=install_requires,
    tests_require=["pytest"],
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
)
