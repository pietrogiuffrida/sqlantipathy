from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
      name="sqlantipathy",
      version="0.0.2",
      url="https://github.com/pietrogiuffrida/sqlantipathy/",
      author="Pietro Giuffrida",
      author_email="pietro.giuffri@gmail.com",
      license="MIT",
      packages=["sqlantipathy"],
      zip_safe=False,
      classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Development Status :: 3 - Alpha",
      ],
      description="Python library to work with SQL DB",
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=[
            "pyodbc",
            "numpy"
      ],
)
