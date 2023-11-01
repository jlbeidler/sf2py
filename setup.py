from setuptools import setup, find_packages, Extension
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
setup(
    name="cemconvert",
    version="0.5.7",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    python_requires='>=3.5',
    scripts=['bin/cemconvert','bin/get_camd_cems','bin/get_camd_cems_bulk'],
    setup_requires=['numpy>=1.19.5','pandas>=1.1.0'],
    install_requires=['numpy>=1.19.5','pandas>=1.1.0'],
    package_data={'cemconvert': ['examples/*.csh','data/*.csv']},
    author_email='beidler.james@epa.gov'
)
