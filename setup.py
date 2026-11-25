from pathlib import Path

from setuptools import find_packages, setup


setup(
    name="sf2py",
    version="0.1.0",
    description="SmartFire 2 fire activity reconciliation tools",
    long_description=(Path(__file__).parent / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    license="GPL-3.0-only",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    package_data={"sf2py": ["sql/*.sql"]},
    python_requires=">=3.9",
    install_requires=[
        "geopandas",
        "numpy",
        "pandas",
        "progress",
        "shapely>=2.0",
        "SQLAlchemy",
    ],
    extras_require={
        "postgres": ["GeoAlchemy2", "psycopg2-binary"],
        "duckdb": ["ibis-framework[duckdb]"],
    },
    entry_points={
        "console_scripts": [
            "sf2py-load-source=sf2py.cli:load_source",
            "sf2py-reconcile=sf2py.cli:reconcile",
            "sf2py-purge-stream=sf2py.cli:purge_stream",
            "sf2py-export-fires=sf2py.cli:export_fires",
            "sf2py-setup-duckdb=sf2py.cli:setup_duckdb",
        ]
    },
)
