from distutils.core import setup

setup(
    name="ktz",
    version="0.1",
    packages=["ktz"],
    license="MIT",
    author="Felix Hamann",
    author_email="felix@hamann.xyz",
    description="KTZ Python Tools",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=[
        "gitpython==3.*",
    ],
)
