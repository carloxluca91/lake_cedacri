import fnmatch
from setuptools import find_packages, setup
from setuptools.command.build_py import build_py as build_py_orig


exclude = ['src.main']


class BuildPy(build_py_orig):

    def find_package_modules(self, package, package_dir):

        modules = super().find_package_modules(package, package_dir)
        return [(pkg, mod, file, ) for (pkg, mod, file, ) in modules
                if not any(fnmatch.fnmatchcase(pkg + '.' + mod, pat=pattern)
                for pattern in exclude)]


def read_me():
    with open("README.md", "r") as f:
        return f.read()


setup(

    name="lake_cedacri",
    version="0.0.1",
    description="Python package that output .parquet file for testing aurora_scala project",
    long_description=read_me(),
    long_description_content_type="text/markdown",
    url="https://github.com/carloxluca91/lake_cedacri.git",
    author="Luca Carloni",
    author_email="carloni.luca91@gmail.com",
    packages=find_packages(),
    cmdclass={'build_py': BuildPy},
    install_requires=["pyspark>=2.2", "pandas>=0.23", "numpy"],
    python_requires=">=3.5"
)